(in-package :cl-mongo-queue)

(define-condition base-error (error)
  ((message :initarg :messsage :reader error-message)))

(define-condition queue-initialization-error (base-error)
  ())

(define-condition queue-add-error (base-error)
  ())

(define-condition queue-get-error (base-error)
  ())

(define-condition queue-ping-error (base-error)
  ())

(define-condition queue-ack-error (base-error)
  ())

(define-condition dead-queue-add-error (base-error)
  ())
