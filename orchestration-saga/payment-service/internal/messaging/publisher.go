package messaging

import (
	commonkafka "saga-pattern/common/kafka"
	"saga-pattern/orchestration-saga/internal/messagingutil"
)

type Publisher = messagingutil.Publisher

type ReplyPublisher = messagingutil.ReplyPublisher

func NewReplyPublisher(publisher Publisher) ReplyPublisher {
	return messagingutil.NewReplyPublisher(publisher, commonkafka.DefaultPaymentRepliesTopic)
}
