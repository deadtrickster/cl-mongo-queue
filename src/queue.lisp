(in-package :cl-mongo-queue)

(defclass queue ()
  ((name :initarg :name :reader queue-name)
   (visibility :initform 30 :initarg :visibility :reader queue-visibility)
   (delay :initform 0 :initarg :delay :reader queue-delay)))

;; queue item interface
(defun queue-item-id (queue-item)
  (assoc-value queue-item :id))

(defun queue-item-ack (queue-item)
  (assoc-value queue-item :ack))

(defun queue-item-payload (queue-item)
  (assoc-value queue-item :payload))

(defun queue-item-tries (queue-item)
  (assoc-value queue-item :tries))

(defun ensure-indexes% (queue)
  (let ((result
          (mind.up::db.command `(("createIndexes" . ,(queue-name queue))
                                 ("indexes" . #((("key" . (("deleted" . 1)
                                                           ("visible" . 1)))
                                                 ("name" . "deleted_visible_index"))
                                                (("key" . (("ack" . 1)))
                                                 ("name" . "ack_index")
                                                 ("unique" . t)
                                                 ("sparse" . t))
                                                (("key" . (("reason" . 1)))
                                                 ("name" . "reason_index")
                                                 ("sparse" . t))))))))
    (values (not (= (gethash "ok" result) 0)) result)))

(defmethod initialize-instance :after ((queue queue) &rest args)
  (declare (ignore args))
  (multiple-value-bind (success result)
      (ensure-indexes% queue)
    (unless success
      (error 'queue-initialization-error :message (gethash "errmsg" result)))))

(defgeneric queue-add (queue payload)
  (:documentation "Add item to queue"))

(defmethod queue-add ((queue queue) payload)
  (let ((result
          (mind:db.insert (queue-name queue) `(("visible" . ,(if (queue-delay queue)
                                                                 (+ (queue-delay queue)
                                                                    (get-universal-time))
                                                                 (get-universal-time)))
                                               ("payload" . ,payload)))))
    (if (= (gethash "ok" result) 0)
        (error 'queue-add-error :message (gethash "errmsg" result)))))

(defgeneric queue-get (queue)
  (:documentation "Get item from queue"))

(defmethod queue-get ((queue queue))
  (let* ((visibility (queue-visibility queue))
         (query `(("visible" . (("$lt" . ,(get-universal-time))))
                  ("deleted" . (("$exists" . :false)))))
         (sort '(("_id" . 1)))
         (update `(("$inc" . (("tries" . 1)))
                   ("$set" . (("ack" . ,(bson:_id))
                              ("visible" . ,(+ visibility (get-universal-time))))))))
    (let ((result
            (db.find-and-modify (queue-name queue)
                                :query query
                                :sort sort
                                :update update
                                :new t)))
      (if (= (gethash "ok" result) 0)
          (error 'queue-get-error :message (gethash "errmsg" result)))

      (if-let ((value (gethash "value" result)))
        `((:id . ,(princ-to-string (gethash "_id" value)))
          (:ack . ,(gethash "ack" value))
          (:payload . ,(gethash "payload" value))
          (:tries . ,(gethash "tries" value)))))))

(defgeneric queue-ping (queue ack &key timeout)
  (:documentation "Tell the queue that you are still alive and continuing to process the message"))

(defmethod queue-ping ((queue queue) ack &key (timeout (queue-visibility queue)))
  (let ((query `(("ack" . ,ack)
                 ("visible" . (("$gt" . ,(get-universal-time))))
                 ("deleted" . (("$exists" . :false)))))
        (update `(("$set" . (("visible" . ,(+ timeout (get-universal-time))))))))
    (let ((result
            (db.find-and-modify (queue-name queue)
                                :query query
                                :update update
                                :new t)))
      (cond
        ((= (gethash "ok" result) 0) 
         (error 'queue-ping-error :message (gethash "errmsg" result)))
        ((null (gethash "value" result))
         (error 'queue-ping-error :message (format nil "QUEUE-PING: possibly unknown ack ~a" ack)))
        (t result)))))

(defgeneric queue-ack (queue ack &key reason)
  (:documentation "Tell the queue item is done"))

(defmethod queue-ack ((queue queue) ack &key (reason "success"))
  (let ((query `(("ack" . ,ack)
                 ("visible" . (("$gt" . ,(get-universal-time))))
                 ("deleted" . (("$exists" . :false)))))
        (update `(("$set" . (("deleted" . ,(get-universal-time))
                             ("reason" . ,reason))))))
    (let ((result
            (db.find-and-modify (queue-name queue)
                                :query query
                                :update update
                                :new t)))
      (cond
        ((= (gethash "ok" result) 0) 
         (error 'queue-ack-error :message (gethash "errmsg" result)))
        ((null (gethash "value" result))
         (error 'queue-ack-error :message (format nil "QUEUE-ACK: possibly unknown ack ~a or item was deleted" ack)))
        (t result)))))

(defgeneric queue-clean (queue)
  (:documentation "Cleanup queue i.e. remove done items"))

(defmethod queue-clean ((queue queue))
  (db.delete (queue-name queue) :selector '(("deleted" . (("$gt" . 0)))) :all t))

(defgeneric queue-total (queue)
  (:documentation "Counts items in queue i.e. wating + inprogress + done"))

(defmethod queue-total ((queue queue))
  (db.count (queue-name queue)))

(defgeneric queue-size (queue)
  (:documentation "Counts waiting items in queue"))

(defmethod queue-size ((queue queue))
  (db.count (queue-name queue) `(("visible" . (("$lt" . ,(get-universal-time))))
                                 ("deleted" . (("$exists" . :false))))))

(defgeneric queue-in-progress (queue)
  (:documentation "Counts in-progress items in queue"))

(defmethod queue-in-progress ((queue queue))
  (db.count (queue-name queue) `(("visible" . ,(get-universal-time))
                                 ("ack" . (("$exists" . t)))
                                 ("deleted" . (("$exists" . :false))))))

(defgeneric queue-done (queue)
  (:documentation "Counts done items in queue"))

(defmethod queue-done ((queue queue))
  (db.count (queue-name queue) `("deleted" . (("$exists" . t)))))


