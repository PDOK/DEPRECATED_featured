(ns pdok.util)

(defmacro with-bench
  "Evaluates expr, followed by bench-out (setting t to time it took in ms) and returning expr"
  [t bench-out & expr]
  `(let [start# (. System (nanoTime))
         ret# (do ~@expr)
         ~t (/ (double (- (. System (nanoTime)) start#)) 1000000.0)]
     ~bench-out
     ret#))
