(in-package :cl-mongo-queue)

(defclass dead-queue (queue)
  ((dead-queue-name :initform nil :initarg :dead-queue-name :reader dead-queue-name)
   (max-retries :initform 5 :initarg :max-retries :reader dead-queue-max-retries)))

(defmethod initialize-instance :after ((queue dead-queue) &rest args)
  (declare (ignore args))
  (unless (dead-queue-name queue)
    (setf (slot-value queue 'dead-queue-name) (concatenate 'string (queue-name queue) "_dead"))))

(defmethod queue-get ((queue dead-queue))
  (if-let ((item (call-next-method)))
    (if (> (queue-item-tries item) (dead-queue-max-retries queue))
        (let ((result (db.insert (dead-queue-name queue) item)))
          (if (= (gethash "ok" result) 0)
              (error 'dead-queue-add-error :message (gethash "errmsg" result))
              (progn (queue-ack queue (queue-item-ack item) :reason "dead")
                     (queue-get queue))))
        item)))

(defgeneric queue-dead-count (dead-queue)
  (:documentation "Count of dead items in queue"))

(defmethod queue-dead-count ((dead-queue dead-queue))
  (db.count (dead-queue-name dead-queue)))
