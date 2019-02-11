<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Mysql\Transactions;

use PDO;
use Ueef\Machina\Exceptions\DriverException;
use Ueef\Machina\Pdo\Interfaces\TransactionInterface;

class RealTransaction implements TransactionInterface
{
    /** @var PDO */
    private $connection;


    public function __construct(PDO $connection)
    {
        $this->connection = $connection;

    }

    public function begin(): void
    {
        if (!$this->connection->beginTransaction()) {
            throw new DriverException("cannot begin transaction");
        }
    }

    public function commit(): void
    {
        if (!$this->connection->commit()) {
            throw new DriverException("cannot commit transaction");
        }
    }

    public function rollback(): void
    {
        if (!$this->connection->rollBack()) {
            throw new DriverException("cannot rollback transaction");
        }
    }
}