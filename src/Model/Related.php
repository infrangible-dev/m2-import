<?php

declare(strict_types=1);

namespace Infrangible\Import\Model;

use Magento\Framework\DB\Adapter\AdapterInterface;
use Psr\Log\LoggerInterface;

/**
 * @author      Andreas Knollmann
 * @copyright   Copyright (c) 2014-2024 Softwareentwicklung Andreas Knollmann
 * @license     http://www.opensource.org/licenses/mit-license.php MIT
 */
abstract class Related
{
    /** @var LoggerInterface */
    protected $logging;

    /** @var Import */
    protected $importer;

    /** @var array */
    private $invalidReasons = [];

    /**
     * @param LoggerInterface $logging
     * @param Import          $importer
     */
    public function __construct(LoggerInterface $logging, Import $importer)
    {
        $this->logging = $logging;
        $this->importer = $importer;
    }

    /**
     * @return Import
     */
    protected function getImporter(): Import
    {
        return $this->importer;
    }

    /**
     * @param AdapterInterface $dbAdapter
     * @param int              $storeId
     * @param array            $element
     *
     * @return void
     */
    abstract public function validate(AdapterInterface $dbAdapter, int $storeId, array $element);

    /**
     * @param string $reason
     *
     * @return void
     */
    protected function addInvalidReason(string $reason)
    {
        $this->invalidReasons[] = $reason;
    }

    /**
     * @return bool
     */
    public function isValid(): bool
    {
        return empty($this->invalidReasons);
    }

    /**
     * @return string
     */
    public function getInvalidReason(): string
    {
        return implode('; ', $this->invalidReasons);
    }

    /**
     * @return array
     */
    abstract public function toArray(): array;
}
