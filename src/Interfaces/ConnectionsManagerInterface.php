<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Interfaces;

use PDO;

interface ConnectionsManagerInterface
{
    public function ping();
    public function acquire(): PDO;
    public function release(PDO $connection);
}