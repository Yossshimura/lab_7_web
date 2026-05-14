<?php

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use Kafka\Producer;
use Kafka\ProducerConfig;
use Kafka\Consumer;
use Kafka\ConsumerConfig;

class QueueManager
{
    private $type;
    private $mainQueue = 'lab7_main_queue';
    private $errorQueue = 'lab7_error_queue';
    private $mainTopic = 'lab7_main_topic';
    private $errorTopic = 'lab7_error_topic';
    private $rabbitChannel;
    private $rabbitConnection;

    public function __construct($type = 'kafka')
    {
        $this->type = $type;
        
        if ($type === 'rabbitmq') {
            $this->initRabbitMQ();
        } else {
            $this->initKafka();
        }
    }

    private function initRabbitMQ()
    {
        $this->rabbitConnection = new AMQPStreamConnection('rabbitmq', 5672, 'guest', 'guest');
        $this->rabbitChannel = $this->rabbitConnection->channel();
        $this->rabbitChannel->queue_declare($this->mainQueue, false, true, false, false);
        $this->rabbitChannel->queue_declare($this->errorQueue, false, true, false, false);
    }

    private function initKafka()
    {
        $this->ensureTopicExists($this->mainTopic);
        $this->ensureTopicExists($this->errorTopic);
    }

    private function ensureTopicExists($topic)
    {
        exec("docker exec lab7_kafka kafka-topics.sh --create --topic $topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 2>/dev/null");
    }

    public function publish($data, $isError = false)
    {
        if ($this->type === 'rabbitmq') {
            $this->publishRabbitMQ($data, $isError);
        } else {
            $this->publishKafka($data, $isError);
        }
    }

    private function publishRabbitMQ($data, $isError = false)
    {
        $queue = $isError ? $this->errorQueue : $this->mainQueue;
        $msg = new AMQPMessage(json_encode($data), ['delivery_mode' => 2]);
        $this->rabbitChannel->basic_publish($msg, '', $queue);
    }

    private function publishKafka($data, $isError = false)
    {
        $topic = $isError ? $this->errorTopic : $this->mainTopic;
        
        $config = ProducerConfig::getInstance();
        $config->setMetadataBrokerList('kafka:9092');
        $config->setProduceInterval(500);

        $producer = new Producer(function() use ($data, $topic) {
            return [[
                'topic' => $topic,
                'value' => json_encode($data),
                'key' => '',
            ]];
        });

        $producer->send(true);
    }

    public function consume($callback, $isError = false)
    {
        if ($this->type === 'rabbitmq') {
            $this->consumeRabbitMQ($callback, $isError);
        } else {
            $this->consumeKafka($callback, $isError);
        }
    }

    private function consumeRabbitMQ($callback, $isError = false)
    {
        $queue = $isError ? $this->errorQueue : $this->mainQueue;
        
        $this->rabbitChannel->basic_consume($queue, '', false, true, false, false, function($msg) use ($callback) {
            $data = json_decode($msg->body, true);
            $callback($data);
        });

        while ($this->rabbitChannel->is_consuming()) {
            $this->rabbitChannel->wait();
        }
    }

    private function consumeKafka($callback, $isError = false)
    {
        $topic = $isError ? $this->errorTopic : $this->mainTopic;
        
        $config = ConsumerConfig::getInstance();
        $config->setMetadataBrokerList('kafka:9092');
        $config->setGroupId('lab7_group');
        $config->setTopics([$topic]);
        $config->setOffsetReset('earliest');

        $consumer = new Consumer();
        $consumer->start(function($topic, $part, $message) use ($callback) {
            $data = json_decode($message['message']['value'], true);
            $callback($data);
        });
    }

    public function getStats()
    {
        if ($this->type === 'rabbitmq') {
            return $this->getRabbitMQStats();
        } else {
            return $this->getKafkaStats();
        }
    }

    private function getRabbitMQStats()
    {
        list($mainQueue, $mainCount, $mainConsumers) = $this->rabbitChannel->queue_declare($this->mainQueue, true);
        list($errorQueue, $errorCount, $errorConsumers) = $this->rabbitChannel->queue_declare($this->errorQueue, true);
        
        return [
            'main_queue' => $mainCount,
            'error_queue' => $errorCount
        ];
    }

    private function getKafkaStats()
    {
        return [
            'main_queue' => $this->getTopicSize($this->mainTopic),
            'error_queue' => $this->getTopicSize($this->errorTopic)
        ];
    }

    private function getTopicSize($topic)
    {
        $output = shell_exec("docker exec lab7_kafka /opt/bitnami/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic 2>/dev/null");
        if (!$output) return 0;
        
        $total = 0;
        $lines = explode("\n", trim($output));
        foreach ($lines as $line) {
            if (preg_match('/:(\d+)$/', $line, $matches)) {
                $total += intval($matches[1]);
            }
        }
        return $total;
    }

    public function __destruct()
    {
        if ($this->type === 'rabbitmq' && $this->rabbitChannel) {
            $this->rabbitChannel->close();
            $this->rabbitConnection->close();
        }
    }
}