<?php


namespace Opmoje\SnsPublisherWithEventType\Publisher;


use Aws\Sns\SnsClient;
use SimpleBus\Asynchronous\Publisher\Publisher;
use SimpleBus\Serialization\Envelope\Serializer\StandardMessageInEnvelopeSerializer;

class SnsPublisher implements Publisher
{
    /** @var SnsClient */
    private $client;

    /** @var string */
    private $topic;

    /** @var StandardMessageInEnvelopeSerializer */
    private $serializer;

    public function __construct(SnsClient $client, StandardMessageInEnvelopeSerializer $serializer, string $topic)
    {
        $this->client = $client;
        $this->topic = $topic;
        $this->serializer = $serializer;
    }

    public function publish($message)
    {
        $this->client->publish(
            [
                'TopicArn' => $this->topic,
                'Message' => $this->serializer->wrapAndSerialize($message),
                'MessageAttributes' => [
                    'event_type' => [
                        'DataType' => 'String',
                        'StringValue' => $message->messageName()
                    ],
                ],
            ]
        );
    }
}