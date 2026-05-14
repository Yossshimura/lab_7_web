<?php
require_once 'vendor/autoload.php';
require_once 'QueueManager.php';

$selectedType = $_GET['type'] ?? 'kafka';
$queue = new QueueManager($selectedType);
$stats = $queue->getStats();

$logFile = 'processed_' . date('Y-m-d') . '.log';
$mainLogs = file_exists($logFile) ? array_reverse(array_slice(file($logFile), 0, 20)) : [];

$errorLogFile = 'error_log.log';
$errorLogs = file_exists($errorLogFile) ? array_reverse(array_slice(file($errorLogFile), 0, 20)) : [];
?>
<!DOCTYPE html>
<html>
<head>
    <title>Статистика очередей</title>
    <style>
        body { font-family: Arial, sans-serif; max-width: 1200px; margin: 30px auto; padding: 20px; }
        .stats { display: flex; gap: 20px; margin-bottom: 30px; }
        .stat-card { background: #f4f4f4; padding: 20px; border-radius: 8px; flex: 1; text-align: center; }
        .stat-number { font-size: 48px; font-weight: bold; color: #e74c3c; }
        .log-entry { background: #f8f9fa; padding: 8px; margin: 5px 0; border-left: 3px solid #2c3e50; font-family: monospace; font-size: 12px; }
        .error-entry { border-left-color: #e74c3c; background: #fdf0f0; }
        button, .btn { background: #2c3e50; color: white; border: none; padding: 10px 20px; border-radius: 4px; cursor: pointer; text-decoration: none; display: inline-block; margin: 5px; }
        button:hover, .btn:hover { background: #e74c3c; }
        .active { background: #e74c3c; }
    </style>
</head>
<body>
    <h1>📊 Статистика очередей</h1>
    
    <div>
        <a href="?type=kafka" class="btn <?= $selectedType === 'kafka' ? 'active' : '' ?>">Kafka (Ваш вариант)</a>
        <a href="?type=rabbitmq" class="btn <?= $selectedType === 'rabbitmq' ? 'active' : '' ?>">RabbitMQ (Штрафное)</a>
    </div>
    
    <div class="stats">
        <div class="stat-card">
            <h3>Основная очередь</h3>
            <div class="stat-number"><?= $stats['main_queue'] ?></div>
            <p>сообщений ожидают обработки</p>
        </div>
        <div class="stat-card">
            <h3>Очередь ошибок</h3>
            <div class="stat-number"><?= $stats['error_queue'] ?></div>
            <p>сообщений с ошибками</p>
        </div>
    </div>
    
    <div>
        <a href="form.html" class="btn">➕ Новая заявка</a>
        <a href="?type=<?= $selectedType ?>" class="btn">🔄 Обновить статистику</a>
    </div>
    
    <div class="log-section">
        <h3>📋 Последние успешно обработанные заявки</h3>
        <?php if (empty($mainLogs)): ?>
            <p>Нет обработанных заявок за сегодня</p>
        <?php else: ?>
            <?php foreach ($mainLogs as $log): ?>
                <div class="log-entry"><?= htmlspecialchars($log) ?></div>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>
    
    <?php if (!empty($errorLogs)): ?>
    <div class="log-section">
        <h3>⚠️ Последние ошибки обработки</h3>
        <?php foreach ($errorLogs as $log): ?>
            <div class="log-entry error-entry"><?= htmlspecialchars($log) ?></div>
        <?php endforeach; ?>
    </div>
    <?php endif; ?>
    
    <hr>
    <h3>🎛 Управление worker'ом</h3>
    <p>Для запуска обработчика выполните:</p>
    <pre style="background: #2c3e50; color: #fff; padding: 10px; border-radius: 4px;">
# Для Kafka (ваш вариант)
docker exec -it lab7_php php worker.php kafka

# Для RabbitMQ (штрафное задание)
docker exec -it lab7_php php worker.php rabbitmq
    </pre>
</body>
</html>