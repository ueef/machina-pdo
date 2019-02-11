<?php
declare(strict_types=1);

namespace Ueef\Machina\Pdo\Interfaces;

interface TransactionInterface
{
    public function begin(): void;
    public function commit(): void;
    public function rollback(): void;
}