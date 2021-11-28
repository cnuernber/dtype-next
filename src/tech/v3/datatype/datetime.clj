(ns tech.v3.datatype.datetime
  "Thorough bindings to java.time.datetime.  Includes packed datatypes and
lifting various datetime datatypes into the datatype system.


A general outline is:

  * There are a set of type-hinted constants you can use in your code however you like.
      - milliseconds-in-day, nanoseconds-in-second, etc.

  * There are type-hinted constructors for the types and ways to take a type and convert it to another type.
     - local-date, instant, zoned-date-time, etc.
     - local-date->instant, instant->zoned-date-time, etc.


  * There are a few generalized functions to create new datetime types or to convert a datetime type
    into milliseconds-since-epoch and back and to add/subtract integer amounts of different chronographic
    units:
     - datetime->milliseconds, milliseconds->datetime
     - plus-temporal-amount, minus-temporal-amount, long-temporal-amount
     - between"
  (:require [tech.v3.datatype.datetime.constants]
            [tech.v3.datatype.datetime.operations]
            [tech.v3.datatype.export-symbols :refer [export-symbols]]))



(export-symbols tech.v3.datatype.datetime.constants
                seconds-in-day
                seconds-in-hour
                seconds-in-minute
                nanoseconds-in-millisecond
                nanoseconds-in-second
                nanoseconds-in-minute
                nanoseconds-in-hour
                nanoseconds-in-day
                nanoseconds-in-week
                milliseconds-in-week
                milliseconds-in-day
                milliseconds-in-hour
                milliseconds-in-minute
                milliseconds-in-second)


(export-symbols tech.v3.datatype.datetime.base
                system-zone-id
                utc-zone-id
                system-zone-offset
                utc-zone-offset
                milliseconds-since-epoch->instant
                seconds-since-epoch->instant
                instant
                instant->milliseconds-since-epoch
                instant->seconds-since-epoch
                instant->microseconds-since-epoch
                microseconds-since-epoch->instant
                local-date-time->local-time
                local-time
                milliseconds-since-epoch->local-time
                local-time->instant
                local-time->seconds
                local-time->milliseconds
                seconds->local-time
                milliseconds->local-time
                instant->local-date-time
                zoned-date-time->instant
                zoned-date-time->milliseconds-since-epoch
                zoned-date-time
                instant->zoned-date-time
                milliseconds-since-epoch->zoned-date-time
                local-date-time->instant
                local-date-time->zoned-date-time
                local-date-time
                milliseconds-since-epoch->local-date-time
                local-date-time->milliseconds-since-epoch
                local-date
                local-date-time->local-date
                local-date->local-date-time
                local-date->days-since-epoch
                local-date->epoch-months
                epoch-months->local-date
                epoch-days->epoch-months
                epoch-months->epoch-days
                days-since-epoch->local-date
                local-date->instant
                local-date->zoned-date-time
                local-date->milliseconds-since-epoch
                local-time->local-date-time
                milliseconds-since-epoch->local-date
                duration
                duration->nanoseconds
                nanoseconds->duration
                duration->milliseconds
                milliseconds->duration)


(export-symbols tech.v3.datatype.datetime.operations
                datetime-datatype?
                duration-datatype?
                long-temporal-field
                plus-temporal-amount
                minus-temporal-amount
                datetime->milliseconds
                milliseconds->datetime
                millisecond-descriptive-statistics
                datetime->epoch
                epoch->datetime
                between
                variable-rolling-window-ranges)
