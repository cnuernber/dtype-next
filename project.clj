(defproject cnuernber/dtype-next "0.1.0-SNAPSHOT"
  :description "Next generation datatype library"
  :url "http://github.com/cnuernber/dtype-next"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure    "1.10.2-alpha1"]
                 [org.ow2.asm/asm        "7.1"]
                 [primitive-math         "0.1.6"]
                 [it.unimi.dsi/fastutil    "8.2.1"]]
  :java-source-paths ["java"]
  :profiles {:uberjar
             {:aot [tech.v3.datatype.main]
              :main tech.v3.datatype.main
              :source-paths ["src" "native_test"]
              :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
              :uberjar-name "dtype-next.jar"}})
