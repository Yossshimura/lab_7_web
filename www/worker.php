<?php
require_once 'vendor/autoload.php';
require_once 'QueueManager.php';

$queueType = $argv[1] ?? 'kafka';
$queue = new QueueManager($queueType);

echo "👷 Рабочий запущен (" . strtoupper($queueType) . ")...\n";
echo "Ожидание сообщений...\n";

$queue->consume(function($data) use ($queue) {
    echo "📥 Получено сообщение: " . json_encode($data) . "\n";
    
    try {
        if (empty($data['name'])) {
            throw new Exception("Имя не может быть пустым");
        }
        
        if ($data['age'] < 1 || $data['age'] > 120) {
            throw new Exception("Некорректный возраст: " . $data['age']);
        }
        
        sleep(2);
        
        $logEntry = json_encode($data) . PHP_EOL;
        file_put_contents('processed_' . date('Y-m-d') . '.log', $logEntry, FILE_APPEND);
        
        echo "✅ Обработано успешно\n";
        
    } catch (Exception $e) {
        echo "❌ Ошибка обработки: " . $e->getMessage() . "\n";
        
        $errorData = $data;
        $errorData['error'] = $e->getMessage();
        $errorData['original_timestamp'] = $errorData['timestamp'];
        $errorData['timestamp'] = date('Y-m-d H:i:s');
        
        $queue->publish($errorData, true);
        echo "📤 Сообщение отправлено в очередь ошибок\n";
    }
});