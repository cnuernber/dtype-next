(defproject cnuernber/dtype-next "0.1.0-SNAPSHOT"
  :description "Next generation datatype library"
  :url "http://github.com/cnuernber/dtype-next"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure    "1.10.2-alpha1"]
                 [org.ow2.asm/asm        "7.1"]
                 [primitive-math         "0.1.6"]]
  :java-source-paths ["java"]
  :repl-options {:init-ns dtype-next.core})
