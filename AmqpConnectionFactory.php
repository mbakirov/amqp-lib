<?php

declare(strict_types=1);

namespace Enqueue\AmqpLib;

use Enqueue\AmqpTools\ConnectionConfig;
use Enqueue\AmqpTools\DelayStrategyAware;
use Enqueue\AmqpTools\DelayStrategyAwareTrait;
use Enqueue\AmqpTools\RabbitMqDlxDelayStrategy;
use Interop\Amqp\AmqpConnectionFactory as InteropAmqpConnectionFactory;
use Interop\Queue\Context;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory as PhpAmqpLibConnectionFactory;
use PhpAmqpLib\Connection\AMQPLazyConnection;
use PhpAmqpLib\Connection\AMQPLazySocketConnection;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Connection\AMQPSSLConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;

class AmqpConnectionFactory implements InteropAmqpConnectionFactory, DelayStrategyAware
{
    use DelayStrategyAwareTrait;

    /**
     * @var ConnectionConfig
     */
    private $config;

    /**
     * @var AbstractConnection
     */
    private $connection;

    /**
     * @param array|string|null $config
     * @see ConnectionConfig for possible config formats and values.
     *
     */
    public function __construct($config = 'amqp:')
    {
        $this->config = (new ConnectionConfig($config))
            ->addSupportedScheme('amqp+lib')
            ->addSupportedScheme('amqps+lib')
            ->addDefaultOption('stream', true)
            ->addDefaultOption('insist', false)
            ->addDefaultOption('login_method', 'AMQPLAIN')
            ->addDefaultOption('login_response', null)
            ->addDefaultOption('locale', 'en_US')
            ->addDefaultOption('keepalive', false)
            ->addDefaultOption('channel_rpc_timeout', 0.)
            ->addDefaultOption('heartbeat_on_tick', true)
            ->parse();

        if (in_array('rabbitmq', $this->config->getSchemeExtensions(), true)) {
            $this->setDelayStrategy(new RabbitMqDlxDelayStrategy());
        }
    }

    /**
     * @return AmqpContext
     */
    public function createContext(): Context
    {
        $context = new AmqpContext($this->establishConnection(), $this->config->getConfig());
        $context->setDelayStrategy($this->delayStrategy);

        return $context;
    }

    public function getConfig(): ConnectionConfig
    {
        return $this->config;
    }

    public function getAmqpConnectionConfig(): AMQPConnectionConfig
    {
        $config = new AMQPConnectionConfig();
        $config->setHost($this->config->getHost());
        $config->setPort($this->config->getPort());
        $config->setUser($this->config->getUser());
        $config->setPassword($this->config->getPass());
        $config->setVhost($this->config->getVHost());

        if ($this->config->isSslOn()) {
            $config->setIsSecure(true);

            $sslOptions = array_filter([
                'CaCert' => $this->config->getSslCaCert(),
                'Cert' => $this->config->getSslCert(),
                'Key' => $this->config->getSslKey(),
                'Verify' => $this->config->isSslVerify(),
                'VerifyName' => $this->config->isSslVerify(),
                'PassPhrase' => $this->getConfig()->getSslPassPhrase(),
                'Ciphers' => $this->config->getOption('ciphers', ''),
            ], function ($value) { return '' !== $value; });

            foreach ($sslOptions as $key => $value) {
                $method = 'setSsl' . $key;
                $config->{$method}($value);
            }
        }

        $config->setInsist($this->config->getOption('insist'));
        $config->setLoginMethod($this->config->getOption('login_method'));

        if ($this->config->getOption('login_response')) {
            $config->setLoginResponse($this->config->getOption('login_response'));
        }

        $config->setLocale($this->config->getOption('locale'));
        $config->setConnectionTimeout($this->config->getConnectionTimeout());
        $config->setChannelRPCTimeout($this->config->getOption('channel_rpc_timeout'));
        $config->setReadTimeout((int)$this->config->getReadTimeout());
        $config->setWriteTimeout((int)$this->config->getWriteTimeout());
        $config->setKeepalive($this->config->getOption('keepalive'));
        $config->setHeartbeat((int)round($this->config->getHeartbeat()));
        $config->setIsLazy($this->config->isLazy());

        $config->setIoType(
            $this->config->getOption('stream') ?
                AMQPConnectionConfig::IO_TYPE_STREAM :
                AMQPConnectionConfig::IO_TYPE_SOCKET
        );

        return $config;
    }

    private function establishConnection(): AbstractConnection
    {
        if (false == $this->connection) {
            $this->connection = PhpAmqpLibConnectionFactory::create($this->getAmqpConnectionConfig());
        }

        return $this->connection;
    }
}
