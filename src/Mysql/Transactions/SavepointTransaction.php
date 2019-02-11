<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Mysql\Transactions;

use PDO;
use PDOException;
use Ueef\Machina\Exceptions\DriverException;
use Ueef\Machina\Pdo\Interfaces\TransactionInterface;

class SavepointTransaction implements TransactionInterface
{
    /** @var string */
    private $id;

    /** @var PDO */
    private $connection;


    public function __construct(PDO $connection)
    {
        $this->id = 's' . random_int(0, PHP_INT_MAX);
        $this->connection = $connection;
    }

    public function begin(): void
    {
        try {
            $this->connection->exec('SAVEPOINT ' . $this->id);
        } catch (PDOException $e) {
            throw new DriverException("cannot begin transaction", 0, $e);
        }
    }

    public function commit(): void
    {
        try {
            $this->connection->exec('RELEASE SAVEPOINT ' . $this->id);
        } catch (PDOException $e) {
            throw new DriverException("cannot commit transaction", 0, $e);
        }
    }

    public function rollback(): void
    {
        try {
            $this->connection->exec('ROLLBACK TO SAVEPOINT ' . $this->id);
        } catch (PDOException $e) {
            throw new DriverException("cannot rollback transaction", 0, $e);
        }
    }
}