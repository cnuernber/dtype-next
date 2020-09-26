(defproject cnuernber/dtype-next "0.3.1"
  :description "A Clojure library designed to aid in the implementation of
high performance algorithms and systems"
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
                             [uncomplicate/neanderthal "0.35.0"]
                             [ch.qos.logback/logback-classic "1.1.3"]]
              :test-paths ["neanderthal" "test"]}
             :codox
             {:dependencies [[codox-theme-rdash "0.1.2"]]
              :plugins [[lein-codox "0.10.7"]]
              :codox {:project {:name "dtype-next"}
                      :metadata {:doc/format :markdown}
                      :themes [:rdash]
                      :source-paths ["src"]
                      :output-path "docs"
                      :doc-paths ["topics"]
                      :source-uri "https://github.com/cnuernber/dtype-next/blob/master/{filepath}#L{line}"
                      :namespaces [tech.v3.datatype tech.v3.datatype.functional
                                   tech.v3.datatype.errors
                                   tech.v3.tensor tech.v3.compute tech.v3.datatype.argops
                                   tech.v3.datatype.bitmap
                                   tech.v3.datatype.datetime
                                   tech.v3.datatype.mmap
                                   tech.v3.tensor.dimensions
                                   tech.v3.tensor.color-gradients
                                   tech.v3.datatype.reductions
                                   tech.v3.datatype.packing
                                   tech.v3.parallel.for
                                   tech.v3.libs.buffered-image
                                   tech.v3.libs.neanderthal]}
              :clean-targets [:target-path "docs"]}
             :travis
             {:test-paths ["test"]
              :dependencies [[org.clojure/core.async "1.3.610"]]}
             :uberjar
             {:aot [tech.v3.datatype.main]
              :source-paths ["src" "native_test"]
              :jvm-opts ["-Dclojure.compiler.direct-linking=true"]
              :uberjar-name "dtype-next.jar"
              :main tech.v3.datatype.main}}
  :aliases {"codox" ["with-profile" "codox,dev" "codox"]})
