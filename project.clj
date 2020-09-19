(defproject cnuernber/dtype-next "0.1.0-SNAPSHOT"
  :description "Next generation datatype library"
  :url "http://github.com/cnuernber/dtype-next"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure            "1.10.2-alpha1"]
                 [org.ow2.asm/asm                "7.1"]
                 [insn                           "0.4.0"]
                 [camel-snake-kebab              "0.4.0"]
                 [primitive-math                 "0.1.6"]
                 [it.unimi.dsi/fastutil          "8.2.1"]
                 [org.xerial.larray/larray-mmap  "0.4.1"]
                 [org.apache.commons/commons-math3 "3.6.1"]
                 [org.roaringbitmap/RoaringBitmap  "0.9.0"]
                 [techascent/tech.resource       "4.6"]]
  :java-source-paths ["java"]
  :profiles {:dev
             {:dependencies [[criterium "0.4.5"]
                             [uncomplicate/neanderthal "0.35.0"]]
              :test-paths ["neanderthal" "test"]}
             :uberjar
             {:aot [tech.v3.tensor]
              :source-paths ["src" "native_test"]
              :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
              :uberjar-name "dtype-next.jar"}})
