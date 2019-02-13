<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Mysql\Transactions;

use PDO;
use PDOException;
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
        try {
            $this->connection->exec('BEGIN');
        } catch (PDOException $e) {
            throw new DriverException("cannot begin transaction", 0, $e);
        }
    }

    public function commit(): void
    {
        try {
            $this->connection->exec('COMMIT');
        } catch (PDOException $e) {
            throw new DriverException("cannot begin transaction", 0, $e);
        }
    }

    public function rollback(): void
    {
        try {
            $this->connection->exec('ROLLBACK');
        } catch (PDOException $e) {
            throw new DriverException("cannot begin transaction", 0, $e);
        }
    }
}