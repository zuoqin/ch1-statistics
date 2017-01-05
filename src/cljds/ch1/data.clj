(ns cljds.ch1.data
  (:refer-clojure :exclude [update])
  (:require [clojure.java.io :as io]
            [incanter.core :as i]
            [incanter.excel :as xls]))

(defmulti load-data identity)

(defmethod load-data :uk [_]
  (-> (io/resource "UK2010.xls")
      (str)
      (xls/read-xls)))

(defmethod load-data :uk-scrubbed [_]
  (->> (load-data :uk)
       (i/$where {"Election Year" {:$ne nil}})))

(defmethod load-data :uk-victors [_]
  (->> (load-data :uk-scrubbed)
       (i/$where {:Con {:$fn number?} :LD {:$fn number?}})
       (i/add-derived-column :victors       [:Con :LD] +)
       (i/add-derived-column :victors-share [:victors :Votes] /)
       (i/add-derived-column :turnout    [:Votes :Electorate] /)))

(defmethod load-data :ru [_]
  (i/conj-rows (-> (io/resource "Russia2011_1of2.xls")
                   (str)
                   (xls/read-xls))
               (-> (io/resource "Russia2011_2of2.xls")
                   (str)
                   (xls/read-xls))))
(defn- map-get
  ([m k]
    (if (keyword? k)
      (or (get m k) (get m (name k)))
      (get m k)))
  ([m k colnames]
    (cond
      (keyword? k)
        (or (get m k) (get m (name k)))
      (number? k)
        (get m (nth colnames k))
      :else
        (get m k))))

(defn- update
  ([m key f] (update-in m [key] f))
  ([m key f & kfs] (apply update (update-in m [key] f) kfs)))



(defn add-derived-column
  "
  This function adds a column to a dataset that is a function of
  existing columns. If no dataset is provided, $data (bound by the
  with-data macro) will be used. f should be a function of the
  from-columns, with arguments in that order.
  Examples:
    (use '(incanter core datasets))
    (def cars (get-dataset :cars))
    (add-derived-column :dist-over-speed [:dist :speed] (fn [d s] (/ d s)) cars)
    (with-data (get-dataset :cars)
      (view (add-derived-column :speed**-1 [:speed] #(/ 1.0 %))))"

  ;; ([column-name from-columns f]
  ;;   (add-derived-column column-name from-columns f $data))
  ([column-name from-columns f data]
    (update data :column-names #(conj % column-name)
            :rows (fn [rows]
                    (mapv (fn [row]
                            (assoc row column-name
                                   (apply f (map #(map-get row %)
                                                 from-columns))))
                          rows)))))


(defmethod load-data :ru-victors [_]
  (->> (load-data :ru)
       (i/rename-cols
        {"Number of voters included in voters list" :electorate
         "Number of valid ballots" :valid-ballots
         "United Russia" :victors})
       (add-derived-column :victors-share
                             [:victors :valid-ballots] i/safe-div)
       (add-derived-column :turnout
                             [:valid-ballots :electorate] /)))


;; (defmethod load-data :ru-victors [_]
;;   (let [
;;         data1 (load-data :ru)
;;         data2 (i/rename-cols
;;         {"Number of voters included in voters list" :electorate
;;          "Number of valid ballots" :valid-ballots
;;          "United Russia" :victors} data1)


;;         data3 (update data2 :column-names #(conj % :victors-share)
;;                 :rows (fn [rows]
;;                         (mapv (fn [row]
;;                                 (assoc row :victors-share
;;                                        (apply i/safe-div (map #(map-get row %)
;;                                                               [:victors :valid-ballots]  ))))
;;                               rows))
;;           )

;;         data4 (update data3 :column-names #(conj % :turnout)
;;                 :rows (fn [rows]
;;                         (mapv (fn [row]
;;                                 (assoc row :turnout
;;                                        (apply i/safe-div (map #(map-get row %)
;;                                                               [:valid-ballots :electorate] ))))
;;                               rows))
;;           )
;;       ]
;;       data4
;;   )

;; )
