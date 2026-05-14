<?php
require_once 'vendor/autoload.php';
require_once 'QueueManager.php';

$queue = new QueueManager('kafka');
$stats = $queue->getStats();

$mainLogs = [];
$errorLogs = [];

if (file_exists('processed_' . date('Y-m-d') . '.log')) {
    $mainLogs = file('processed_' . date('Y-m-d') . '.log');
    $mainLogs = array_reverse(array_slice($mainLogs, 0, 20));
}

if (file_exists('error_log.log')) {
    $errorLogs = file('error_log.log');
    $errorLogs = array_reverse(array_slice($errorLogs, 0, 20));
}
?>
<!DOCTYPE html>
<html>
<head>
    <title>Статистика очередей - Kafka</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 1200px;
            margin: 30px auto;
            padding: 20px;
        }
        .stats {
            display: flex;
            gap: 20px;
            margin-bottom: 30px;
        }
        .stat-card {
            background: #f4f4f4;
            padding: 20px;
            border-radius: 8px;
            flex: 1;
            text-align: center;
        }
        .stat-card h3 {
            margin: 0 0 10px 0;
            color: #2c3e50;
        }
        .stat-number {
            font-size: 48px;
            font-weight: bold;
            color: #e74c3c;
        }
        .log-section {
            margin-bottom: 30px;
        }
        .log-section h3 {
            background: #2c3e50;
            color: white;
            padding: 10px;
            border-radius: 4px;
        }
        .log-entry {
            background: #f8f9fa;
            padding: 8px;
            margin: 5px 0;
            border-left: 3px solid #2c3e50;
            font-family: monospace;
            font-size: 12px;
            word-break: break-all;
        }
        .error-entry {
            border-left-color: #e74c3c;
            background: #fdf0f0;
        }
        button {
            background: #2c3e50;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            margin: 10px 0;
        }
        button:hover {
            background: #e74c3c;
        }
        nav a {
            display: inline-block;
            margin: 10px 10px 0 0;
            padding: 10px 15px;
            background: #3498db;
            color: white;
            text-decoration: none;
            border-radius: 4px;
        }
        nav a:hover {
            background: #2980b9;
        }
    </style>
</head>
<body>
    <h1>📊 Статистика очередей (Kafka)</h1>
    
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
    
    <nav>
        <a href="form.html">➕ Новая заявка</a>
        <a href="index.php">🔄 Обновить статистику</a>
    </nav>
    
    <div class="log-section">
        <h3>📋 Последние успешно обработанные заявки (сегодня)</h3>
        <?php if (empty($mainLogs)): ?>
            <p>Нет обработанных заявок за сегодня</p>
        <?php else: ?>
            <?php foreach ($mainLogs as $log): ?>
                <div class="log-entry">
                    <?= htmlspecialchars($log) ?>
                </div>
            <?php endforeach; ?>
        <?php endif; ?>
    </div>
    
    <?php if (!empty($errorLogs)): ?>
    <div class="log-section">
        <h3>⚠️ Последние ошибки обработки</h3>
        <?php foreach ($errorLogs as $log): ?>
            <div class="log-entry error-entry">
                <?= htmlspecialchars($log) ?>
            </div>
        <?php endforeach; ?>
    </div>
    <?php endif; ?>
    
    <hr>
    <h3>🎛 Управление worker'ом</h3>
    <p>Для запуска обработчика выполните в командной строке:</p>
    <pre style="background: #2c3e50; color: #fff; padding: 10px; border-radius: 4px;">
docker exec -it lab7_php php worker.php kafka
    </pre>
    <p><small>Worker должен работать постоянно. Для остановки нажмите Ctrl+C.</small></p>
</body>
</html>