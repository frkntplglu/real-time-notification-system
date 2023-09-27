package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/frkntplglu/notification-system/internal/models"
	"github.com/gin-gonic/gin"
)

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

var ErrUserNotFoundInProducer = errors.New("user not found")

func findByUserID(id int, users []models.User) (models.User, error) {
	for _, user := range users {
		if user.ID == id {
			return user, nil
		}
	}

	return models.User{}, ErrUserNotFoundInProducer
}

func getIdFromRequest(formValue string, ctx *gin.Context) (int, error) {
	id, err := strconv.Atoi(ctx.PostForm(formValue))

	if err != nil {
		return 0, fmt.Errorf("failed to parse Id from form value %s: %w", formValue, err)
	}

	return id, nil
}

func sendKafkaMessage(producer sarama.SyncProducer, users []models.User, ctx *gin.Context, fromId, toId int) error {
	message := ctx.PostForm("message")
	fromUser, err := findByUserID(fromId, users)

	if err != nil {
		return err
	}

	toUser, err := findByUserID(toId, users)

	if err != nil {
		return err
	}

	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: message,
	}

	notificationJSON, err := json.Marshal(notification)

	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(notificationJSON),
	}

	partition, offset, err := producer.SendMessage(msg)

	fmt.Println("partition : ", partition)
	fmt.Println("offset : ", offset)

	return err
}

func sendMessageHandler(producer sarama.SyncProducer, users []models.User) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		fromId, err := getIdFromRequest("fromId", ctx)

		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
			return
		}

		toId, err := getIdFromRequest("toId", ctx)

		if err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"message": err.Error()})
		}

		err = sendKafkaMessage(producer, users, ctx, fromId, toId)

		if errors.Is(err, ErrUserNotFoundInProducer) {
			ctx.JSON(http.StatusNotFound, gin.H{"message": "User not found"})
			return
		}

		if err != nil {
			ctx.JSON(http.StatusInternalServerError, gin.H{
				"message": err.Error(),
			})
			return
		}

		ctx.JSON(http.StatusOK, gin.H{"message": "Notification sent successfully!"})

	}
}

func setupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)

	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}

	return producer, nil
}

func main() {
	users := []models.User{
		{ID: 1, Name: "Furkan"},
		{ID: 2, Name: "Ayşe"},
		{ID: 3, Name: "Emir"},
		{ID: 4, Name: "Gaye"},
	}

	producer, err := setupProducer()

	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}

	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.POST("/send", sendMessageHandler(producer, users))

	fmt.Printf("Kafka producer 📨 started at http://localhost%s\n", ProducerPort)

	if err := router.Run(ProducerPort); err != nil {
		log.Printf("failed to run the server: %v", err)
	}
}
