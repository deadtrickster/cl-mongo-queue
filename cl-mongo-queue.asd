;;; Copyright (C) 2014  Ilya Khaprov https://github.com/deadtrickster
;;;
;;; See LICENSE for details.

(defpackage :cl-dropbox-system
  (:use :cl :asdf))

(in-package :cl-dropbox-system)

(defsystem :cl-mongo-queue
  :description "MongoDB based queue, port of https://github.com/chilts/mongodb-queue"
  :license "MIT"
  :author "Ilya Khaprov"
  :version "0.1.0"
  :depends-on (:pinkiepie)
  :serial t
  :components
  ((:module "src"
    :components
    ((:file "package")
     (:file "conditions")
     (:file "queue")
     (:file "dead-queue")))))
