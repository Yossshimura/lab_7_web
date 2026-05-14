<?php
require_once 'vendor/autoload.php';
require_once 'QueueManager.php';

$queueType = $_POST['queue_type'] ?? 'kafka';

$queue = new QueueManager($queueType);

$data = [
    'name' => htmlspecialchars($_POST['name'] ?? 'Без имени'),
    'age' => intval($_POST['age'] ?? 0),
    'topic' => htmlspecialchars($_POST['topic'] ?? 'Не выбрана'),
    'prize' => isset($_POST['prize']) ? 'Да' : 'Нет',
    'difficulty' => htmlspecialchars($_POST['difficulty'] ?? 'Не выбрана'),
    'timestamp' => date('Y-m-d H:i:s'),
    'ip' => $_SERVER['REMOTE_ADDR'] ?? 'unknown'
];

try {
    $queue->publish($data);
    echo json_encode(['success' => true, 'message' => '✅ Сообщение отправлено в очередь (' . $queueType . ')']);
} catch (Exception $e) {
    echo json_encode(['success' => false, 'message' => '❌ Ошибка: ' . $e->getMessage()]);
}