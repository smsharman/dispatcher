(ns dispatcher.core
  (:require [uswitch.lambada.core :refer [deflambdafn]]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [cognitect.aws.client.api :as aws])
  (:gen-class))

(def routeTableKey "")

(def testMessage1 {
                   :eventId "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :parentId "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :originId "7a5a815b-2e52-4d40-bec8-c3fc06edeb36"
                   :userId "1"
                   :orgId "1"
                   :eventVersion 1
                   :eventAction "event1"
                   :eventData {
                               :key1 "value1"
                               :key2 "value2"
                               }
                   :eventTimestamp "2018-10-09T12:24:03.390+0000"
                   })

(defn generate-return [statuscode message]
  "Generate a simple Lambda status return"
  {:status statuscode :message message})

(defn handle-event
  [event]
  (let [eventaction (get event :eventAction)]
    (println "Got the following event : " (print-str event))
    (println "eventAction is: " eventaction)
    (generate-return 200 "ok")))

(deflambdafn dispatcher.core.Route
             [in out ctx]
             "Takes a JSON event in standard Synergy Event form, convert to map and send to routing function"
             (let [event (json/read (io/reader in) :key-fn keyword)
                   res (handle-event event)]
               (with-open [w (io/writer out)]
                 (json/write res w))))
