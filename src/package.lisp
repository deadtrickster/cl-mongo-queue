(in-package :cl-user)

(defpackage :cl-mongo-queue
  (:use :cl :alexandria :mind)
  (:nicknames :mq)
  (:export #:queue
           #:dead-queue
           ;; queue operations
           #:queue-add
           #:queue-get
           #:queue-ping
           #:queue-ack
           #:queue-clean
           #:queue-total
           #:queue-size
           #:queue-in-progress
           #:queue-done
           #:queue-dead-count
           ;; queue item interface
           #:queue-item-id
           #:queue-item-ack
           #:queue-item-payload
           #:queue-item-tries
           ))
