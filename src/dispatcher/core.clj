(ns dispatcher.core
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [cognitect.aws.client.api :as aws]
            [synergy-specs.events :as synspec]
            [clojure.spec.alpha :as s]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:gen-class))

;; Declare clients for AWS services required
(def ssm (aws/client {:api :ssm}))

(def s3 (aws/client {:api :s3}))

(def sns (aws/client {:api :sns}))

(def routeTableParameters {
                           :bucket "synergyDispatchBucket"
                           :filename "synergyDispatchRoutingTable"
                           :arn-prefix "synergyDispatchTopicArnRoot"
                           :event-store-topic "synergyEventStoreTopic"
                          })

(def routeTable (atom {}))

(def snsArnPrefix (atom ""))

(def eventStoreTopic (atom ""))

(defn getRouteTableParametersFromSSM []
  "Look up values in the SSM parameter store to be later used by the routing table"
  (let [tableBucket (get-in (aws/invoke ssm {:op :GetParameter
                                                             :request {:Name (get routeTableParameters :bucket)}})
                            [:Parameter :Value])
        tableFilename (get-in (aws/invoke ssm {:op :GetParameter
                                                               :request {:Name (get routeTableParameters :filename)}})
                            [:Parameter :Value])
        snsPrefix (get-in (aws/invoke ssm {:op :GetParameter
                                                           :request {:Name (get routeTableParameters :arn-prefix)}})
                            [:Parameter :Value])
        evStoreTopic (get-in (aws/invoke ssm {:op :GetParameter
                                           :request {:Name (get routeTableParameters :event-store-topic)}})
                          [:Parameter :Value])
        ]
    ;; //TODO: add error handling so if for any reason we can't get the values, this is noted
    {:tableBucket tableBucket :tableFilename tableFilename :snsPrefix snsPrefix :eventStoreTopic evStoreTopic}))

(defn setEventStoreTopic [parameter-map]
  "Set the eventStoreTopic atom with the required value"
  (swap! eventStoreTopic str (get parameter-map :eventStoreTopic)))

(defn setArnPrefix [parameter-map]
  "Set the snsArnPrefix atom with the required value"
  (swap! snsArnPrefix str (get parameter-map :snsPrefix)))

(defn loadRouteTable [parameter-map]
  "Load routing table from S3, input is a map with :tableBucket :tableFilename and :snsPrefix"
  ;; Get file from S3 and turn it into a map
  (let [tableBucket (get parameter-map :tableBucket)
        tableFilename (get parameter-map :tableFilename)
        rawRouteMap (json/read (io/reader
                                 (get (aws/invoke s3 {:op :GetObject
                                               :request {:Bucket tableBucket :Key tableFilename}}) :Body)) :key-fn keyword)]
    (swap! routeTable merge @routeTable rawRouteMap)))

;; Valid message
(def testMessage1 {
                   :eventId        "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :parentId       "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :originId       "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :userId         "1"
                   :orgId          "1"
                   :eventVersion   1
                   :eventAction    "event1"
                   :eventData      {
                                    :key1 "value1"
                                    :key2 "value2"
                                    }
                   :eventTimestamp "2018-10-09T12:24:03.390+0000"
                   })

;; Invalid message - missing keys
(def testMessage2 {
                   :eventId        "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :parentId       "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :originId       "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :eventAction    "event1"
                   :eventData      {
                                    :key1 "value1"
                                    :key2 "value2"
                                    }
                   :eventTimestamp "2018-10-09T12:24:03.390+0000"
                   })

;; Invalid message - unknown version number
(def testMessage3 {
                   :eventId        "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :parentId       "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :originId       "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :userId         "1"
                   :orgId          "1"
                   :eventVersion   2
                   :eventAction    "event1"
                   :eventData      {
                                    :key1 "value1"
                                    :key2 "value2"
                                    }
                   :eventTimestamp "2018-10-09T12:24:03.390+0000"
                   })

(defn gen-status-map
  "Generate a status map from the values provided"
  [status-code status-message return-value]
  (let [return-status-map {:status status-code :description status-message :return-value return-value}]
    return-status-map))

(defn generate-lambda-return [statuscode message]
  "Generate a simple Lambda status return"
  {:status statuscode :message message})

(defn validate-message [inbound-message]
  (if (s/valid? ::synspec/synergyEvent inbound-message)
    (gen-status-map true "valid-inbound-message" {})
    (gen-status-map false "invalid-inbound-message" (s/explain-data ::synspec/synergyEvent inbound-message))))

(defn set-up-route-table []
  (reset! routeTable {})
  (reset! snsArnPrefix "")
  (reset! eventStoreTopic "")
  (info "Routing table not found - setting up (probably first run for this Lambda instance")
  (let [route-paraneters (getRouteTableParametersFromSSM)]
    (setArnPrefix route-paraneters)
    (setEventStoreTopic route-paraneters)
    (loadRouteTable route-paraneters)))

(defn get-route-table-entries [event]
  (let [eventAction (get event ::synspec/eventAction)
        eventVersion (get event ::synspec/eventVersion)
        eventId (get event ::synspec/eventId)
        eventLookup (str eventAction "_" eventVersion)
        eventTopics (get-in @routeTable [:synergyRoutes (keyword eventLookup) :dispatchTopics])]
    (if (nil? eventTopics)
      (gen-status-map false "route-table-entry-not-present" {:eventId eventId :eventAction eventAction :eventVersion eventVersion :eventLookup eventLookup})
      (gen-status-map true "route-table-entry-found" {:topics eventTopics}))))


(defn send-to-topic
  ([thisTopic thisEvent]
   (send-to-topic thisTopic thisEvent ""))
  ([topic event note]
   (let [thisEventId (get event ::synspec/eventId)
         jsonEvent (json/write-str event)
         eventSNS (str @snsArnPrefix topic)
         snsSendResult (aws/invoke sns {:op :Publish :request {:TopicArn eventSNS
                                                               :Message  jsonEvent}})]
     (if (nil? (get snsSendResult :MessageId))
       (do
         (info "Error dispatching event to topic : " topic " (" note ") : " event)
         (gen-status-map false "error-dispatching-to-topic" {:eventId thisEventId
                                                             :error snsSendResult}))
       (do
         (info "Dispatching event to topic : " eventSNS " (" note ") : " event)
         (gen-status-map true "dispatched-to-topic" {:eventId   thisEventId
                                                     :messageId (get snsSendResult :MessageId)}))))))


(defn dispatch-event [event topics]
  (send-to-topic @eventStoreTopic event "Dispatching to topics")
  (doseq [topic topics]
    (send-to-topic topic event "")))
  (gen-status-map true "event-dispatched" {})


(defn route-event [event]
  "Route an inbound event to a given set of destination topics"
  (if (empty? @routeTable)
    (set-up-route-table))
  (let [validateEvent (validate-message event)]
    (if (true? (get validateEvent :status))
      (let [routeTableEntries (get-route-table-entries event)]
        (if (true? (get routeTableEntries :status))
          (dispatch-event event (get-in routeTableEntries [:return-value :topics]))
          (send-to-topic @eventStoreTopic event "No routing topic")))
      (send-to-topic @eventStoreTopic event (str "Event not valid : " (get validateEvent :return-value))))))


(defn handle-event
  [event]
  (let [nsevent (synergy-specs.events/wrap-std-event event)]
    (info "Received the following event : " (print-str nsevent))
    (route-event nsevent)
    (generate-lambda-return 200 "ok")))

(deflambdafn dispatcher.core.Route
             [in out ctx]
             "Takes a JSON event in standard Synergy Event form, convert to map and send to routing function"
             (let [event (json/read (io/reader in) :key-fn keyword)
                   res (handle-event event)]
               (with-open [w (io/writer out)]
                 (json/write res w))))
