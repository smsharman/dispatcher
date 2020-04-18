(ns dispatcher.core
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [cognitect.aws.client.api :as aws]
            [synergy-specs.events :as synspec]
            [clojure.spec.alpha :as s]
            [synergy-events-stdlib.core :as stdlib]
            [taoensso.timbre :as timbre
             :refer [log trace debug info warn error fatal report
                     logf tracef debugf infof warnf errorf fatalf reportf
                     spy get-env]])
  (:gen-class))

;; Declare clients for AWS services required
(def ssm (aws/client {:api :ssm}))

(def s3 (aws/client {:api :s3}))

(def sns (aws/client {:api :sns}))

(def routeTable (atom {}))

(def snsArnPrefix (atom ""))

(def eventStoreTopic (atom ""))

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
                   :eventTimestamp "2020-04-17T11:23:10.904Z"
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
                   :eventTimestamp "2020-04-17T11:23:10.904Z"
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
                   :eventTimestamp "2020-04-17T11:23:10.904Z"
                   })


(defn get-route-table-entries [event]
  (let [eventAction (get event ::synspec/eventAction)
        eventVersion (get event ::synspec/eventVersion)
        eventId (get event ::synspec/eventId)
        eventLookup (str eventAction "_" eventVersion)
        eventTopics (get-in @routeTable [:synergyRoutes (keyword eventLookup) :dispatchTopics])]
    (if (nil? eventTopics)
      (stdlib/gen-status-map false "route-table-entry-not-present" {:eventId eventId :eventAction eventAction :eventVersion eventVersion :eventLookup eventLookup})
      (stdlib/gen-status-map true "route-table-entry-found" {:topics eventTopics}))))


(defn dispatch-event [event topics]
  (stdlib/send-to-topic @eventStoreTopic event @snsArnPrefix sns "Dispatching to topics")
  (doseq [topic topics]
    (stdlib/send-to-topic topic event @snsArnPrefix sns)))
  (stdlib/gen-status-map true "event-dispatched" {})


(defn route-event [event]
  "Route an inbound event to a given set of destination topics"
  (if (empty? @routeTable)
    (do
    (stdlib/set-up-route-table routeTable ssm s3)
    (stdlib/set-up-topic-table snsArnPrefix eventStoreTopic ssm)))
  (let [validateEvent (stdlib/validate-message event)]
    (if (true? (get validateEvent :status))
      (let [routeTableEntries (get-route-table-entries event)]
        (if (true? (get routeTableEntries :status))
          (dispatch-event event (get-in routeTableEntries [:return-value :topics]))
          (stdlib/send-to-topic @eventStoreTopic event @snsArnPrefix sns "No routing topic")))
      (stdlib/send-to-topic @eventStoreTopic event @snsArnPrefix sns (str "Event not valid : " (get validateEvent :return-value))))))


(defn handle-event
  [event]
  (let [cevent (json/read-str (get (get (first (get event :Records)) :Sns) :Message) :key-fn keyword)
        nsevent (synspec/wrap-std-event cevent)]
    (info "Received the following event : " (print-str nsevent))
    (route-event nsevent)
    (stdlib/generate-lambda-return 200 "ok")))

(deflambdafn dispatcher.core.Route
             [in out ctx]
             "Takes a JSON event in standard Synergy Event form, convert to map and send to routing function"
             (let [event (json/read (io/reader in) :key-fn keyword)
                   res (handle-event event)]
               (with-open [w (io/writer out)]
                 (json/write res w))))
