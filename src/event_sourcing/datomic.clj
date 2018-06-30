(ns event-sourcing.datomic
  "clojure与datomic实现事件溯源的样例"
  (:require [datomic.api :as d]))

(def db-schema
  "Datomic数据结构定义(类似于sql的数据表定义)"
  [#:db {:ident       :order/id
         :doc         "单号"
         :valueType   :db.type/uuid
         :cardinality :db.cardinality/one
         :unique      :db.unique/identity}
   #:db {:ident       :order/operator
         :doc         "操作人"
         :valueType   :db.type/string
         :cardinality :db.cardinality/one}
   #:db {:ident       :order/time
         :doc         "操作时间"
         :valueType   :db.type/instant
         :cardinality :db.cardinality/one}
   #:db {:ident       :order/location
         :doc         "操作地点"
         :valueType   :db.type/string
         :cardinality :db.cardinality/one}
   #:db {:ident       :order/action
         :doc         "操作动作"
         :valueType   :db.type/string
         :cardinality :db.cardinality/one}])

(defn init-db
  "初始化数据库连接,传入数据库连接地址,返回连接"
  [url]
  (d/create-database url)
  (def conn (d/connect url))
  (d/transact conn db-schema)
  conn)

(defn query-order
  "查询订单的数据,正常的查询,查询到的是最新的状态(类似于sql,可以用where来定位数据)"
  [db order-id]
  (d/q '[:find (pull ?e [*]) .
         :in $ ?orderid
         :where [?e :order/id ?orderid]]
       db order-id))

; 示例执行
(comment

  (do
    ; 初始化数据库
    (def url (str "datomic:mem:" (java.util.UUID/randomUUID)))
    (def conn (init-db url))

    ; 存入三个事件的数据: 下单, 分配仓库, 发货
    (def order-id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c")
    @(d/transact conn [{:order/id       order-id
                        :order/operator "我"
                        :order/time     #inst"2018-07-01T12:00:00"
                        :order/action   "下单"}])

    @(d/transact conn [{:order/id       order-id
                        :order/operator "物流系统"
                        :order/time     #inst"2018-07-01T12:10:00"
                        :order/action   "分派订单到A仓库"}])

    @(d/transact conn [{:order/id       order-id
                        :order/operator "发货员B"
                        :order/time     #inst"2018-07-01T12:20:00"
                        :order/location "A仓库"
                        :order/action   "发往C物流中心"}]))

  ; 查询这个订单的数据,正常的查询,查询到的是最新的状态
  (query-order (d/db conn) order-id)

  => {:db/id 17592186045418,
      :order/id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c",
      :order/operator "发货员B",
      :order/time #inst"2018-07-01T12:20:00.000-00:00",
      :order/location "A仓库",
      :order/action "发往C物流中心"}

  ; 查历史记录
  (def history
    (d/q '[:find ?tx ?attr ?v ?t    ; ?tx是某一次数据更新的id, ?attr是属性, ?v是属性值, ?t为是否更新后的值
           :in $ ?id
           :where
           [?e :order/id ?id]
           [?e ?a ?v ?tx ?t]
           [?a :db/ident ?attr]]
         (d/history (d/db conn)) order-id))

  history

  => #{[13194139534316 :order/operator "物流系统" false]
       [13194139534316 :order/operator "发货员B" true]
       [13194139534316 :order/location "A仓库" true]
       [13194139534313 :order/action "下单" true]
       [13194139534315 :order/operator "物流系统" true]
       [13194139534313 :order/id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c" true]
       [13194139534315 :order/action "下单" false]
       [13194139534316 :order/action "发往C物流中心" true]
       ...
       }

  ; 把历史记录按数据执行的id进行聚合
  (->> history
       (sort)
       (partition-by first))  ; 按每行数据的第一个值来聚合

  => (([13194139534313 :order/action "下单" true]
       [13194139534313 :order/id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c" true]
       [13194139534313 :order/operator "我" true]
       [13194139534313 :order/time #inst"2018-07-01T12:00:00.000-00:00" true])
      ([13194139534315 :order/action "下单" false]
       [13194139534315 :order/action "分派订单到A仓库" true]
       [13194139534315 :order/operator "我" false]
       [13194139534315 :order/operator "物流系统" true]
       [13194139534315 :order/time #inst"2018-07-01T12:00:00.000-00:00" false]
       [13194139534315 :order/time #inst"2018-07-01T12:10:00.000-00:00" true])
      ([13194139534316 :order/action "分派订单到A仓库" false]
       [13194139534316 :order/action "发往C物流中心" true]
       [13194139534316 :order/location "A仓库" true]
       [13194139534316 :order/operator "发货员B" true]
       [13194139534316 :order/operator "物流系统" false]
       [13194139534316 :order/time #inst"2018-07-01T12:10:00.000-00:00" false]
       [13194139534316 :order/time #inst"2018-07-01T12:20:00.000-00:00" true]))

  ; 把历史记录按数据执行的id进行聚合,并且只留下每次更新后的数据(最后一个值为true的数据行)
  (->> history
       (filter #(last %))    ; 比上一次处理多了这行过滤处理
       (sort)
       (partition-by first))

  => (([13194139534313 :order/action "下单" true]
       [13194139534313 :order/id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c" true]
       [13194139534313 :order/operator "我" true]
       [13194139534313 :order/time #inst"2018-07-01T12:00:00.000-00:00" true])
      ([13194139534315 :order/action "分派订单到A仓库" true]
       [13194139534315 :order/operator "物流系统" true]
       [13194139534315 :order/time #inst"2018-07-01T12:10:00.000-00:00" true])
      ([13194139534316 :order/action "发往C物流中心" true]
       [13194139534316 :order/location "A仓库" true]
       [13194139534316 :order/operator "发货员B" true]
       [13194139534316 :order/time #inst"2018-07-01T12:20:00.000-00:00" true]))

  ; 对一次数据执行id的历史记录提取属性和属性值
  (defn simplify
    [tx-data]
    (->> (map (fn [[_ attr value _]] [attr value]) tx-data)
         (into {})))

  (simplify [[13194139534313 :order/action "下单" true]
             [13194139534313 :order/id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c" true]
             [13194139534313 :order/operator "我" true]
             [13194139534313 :order/time #inst"2018-07-01T12:00:00.000-00:00" true]])
  => #:order{:action "下单",
             :id #uuid "287fc397-a432-49d7-9068-d7499cd2e28c",
             :operator "我",
             :time #inst "2018-07-01T12:00:00.000-00:00"}

  ; 对历史记录提取属性和属性值,拼成每次数据执行的数据
  (->> history
       (filter #(last %))
       (sort)
       (partition-by first)
       (map #(simplify %)))  ; 对每一次数据执行的数据进行提取属性和属性值

  => (#:order{:action "下单",
              :id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c",
              :operator "我",
              :time #inst"2018-07-01T12:00:00.000-00:00"}
       #:order{:action "分派订单到A仓库",
               :operator "物流系统",
               :time #inst"2018-07-01T12:10:00.000-00:00"}
       #:order{:action "发往C物流中心",
               :location "A仓库",
               :operator "发货员B",
               :time #inst"2018-07-01T12:20:00.000-00:00"})

  ; 对比开始时存到数据库的三个事件数据,还有点不一样?后面的两个事件少了:order/id的数据
  ; 是的,每次数据执行,只记录了变更的数据
  {:order/id       order-id
   :order/operator "我"
   :order/time     #inst"2018-07-01T12:00:00"
   :order/action   "下单"}

  {:order/id       order-id
   :order/operator "物流系统"
   :order/time     #inst"2018-07-01T12:10:00"
   :order/action   "分派订单到A仓库"}

  {:order/id       order-id
   :order/operator "发货员B"
   :order/time     #inst"2018-07-01T12:20:00"
   :order/location "A仓库"
   :order/action   "发往C物流中心"}

  ; 如果要查看某次数据执行后的订单状态,那么,就要从头开始把数据更新事件进行叠加了,头大
  ; 好消息是,clojure中map数据的叠加是很好处理的:一次叠加只要用一次merge函数即可
  (defn merge-up
    [tx-list]
    (reduce
      (fn [result next-tx]
        (conj result (merge (last result) next-tx)))
      [(first tx-list)] (rest tx-list)))

  (->> history
       (filter #(last %))
       (sort)
       (partition-by first)
       (map #(simplify %))
       (merge-up))

  => [#:order{:action "下单",
              :id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c",
              :operator "我",
              :time #inst"2018-07-01T12:00:00.000-00:00"}
      #:order{:action "分派订单到A仓库",
              :id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c",
              :operator "物流系统",
              :time #inst"2018-07-01T12:10:00.000-00:00"}
      #:order{:action "发往C物流中心",
              :id #uuid"287fc397-a432-49d7-9068-d7499cd2e28c",
              :operator "发货员B",
              :time #inst"2018-07-01T12:20:00.000-00:00",
              :location "A仓库"}]

  ; 这下,追溯的事件数据就跟当初存入的数据一样了(map数据是无序的)
  ; 上面写了那么多,其实,整理下就下面几个函数:
  (defn query-history
    [db order-id]
    (d/q '[:find ?tx ?attr ?v ?t    ; ?tx是某一次数据更新的id, ?attr是属性, ?v是属性值, ?t为是否更新后的值
           :in $ ?id
           :where
           [?e :order/id ?id]
           [?e ?a ?v ?tx ?t]
           [?a :db/ident ?attr]]
         (d/history db) order-id))

  (defn simplify
    [tx-data]
    (->> (map (fn [[_ attr value _]] [attr value]) tx-data)
         (into {})))

  (defn merge-up
    [tx-list]
    (reduce
      (fn [result next-tx]
        (conj result (merge (last result) next-tx)))
      [(first tx-list)] (rest tx-list)))

  (defn sourcing
    [db order-id]
    (->> (query-history db order-id)
         (filter #(last %))
         (sort)
         (partition-by first)
         (map #(simplify %))
         (merge-up)))

  (sourcing (d/db conn) #uuid"287fc397-a432-49d7-9068-d7499cd2e28c")


  ; 另外,历史记录还可以查到每次数据更新发生的时间
  (d/q '[:find ?tx ?attr ?v ?t ?inst   ; ?inst是数据更新发生的时间
         :in $ ?id
         :where
         [?e :order/id ?id]
         [?e ?a ?v ?tx ?t]
         [?a :db/ident ?attr]
         [?tx :db/txInstant ?inst]]
       (d/history (d/db conn)) order-id)
  =>
  #{[13194139534316 :order/action "分派订单到A仓库" false #inst"2018-06-29T12:12:14.731-00:00"]
    [13194139534313 :order/time #inst"2018-07-01T12:00:00.000-00:00" true #inst"2018-06-29T12:12:14.728-00:00"]
    ...}

  ; 删除数据库
  (d/delete-database url)
  )
