var amqp = require('amqplib');

class RabbitmqWrapper {
    ampq = null
    /**
     * Connect to rabbitmq connection string
     * @param {*} url 
     */
    constructor(url) {
        this.url = url;
    }
    /**
     * Connect to RabbitMQ server via connection string
     * @param {string} url [optional] Connection string, default is localhost
     * @returns 
     */
    connect = (url = null) => {
        return amqp.connect(url ?? this.url).then(conn => {
            this.ampq = conn;
            return { conn };
        }).catch((err) => {
            console.error('[%s] %s', 'Error', err.message)
            return err
        });
    }
    
    /**
     * Attach ampq connection to handle by this wrapper
     * @param {ampq} ampq ampq connection
     */
    attach = (ampq) => {
        this.ampq = ampq;
    }

    /**
     * Quick function to consume to Exchange channel
     * @param {object} option Params to assert to channel
     *  - (string) queue: Name of queue
     *  - (string) type: type of Exchange, default is "fanout"
     *  - (object) on: Callback for some events
     * @param {function} consumer Callback function to handle payload
     */
    consumeToExchange = ({ queue, type = 'fanout', on = { channelAsserted: () => { }, queueAsserted: () => { } } }, consumer) => {
        this.assertExchange({ queue: queue }, ({ channel }) => {
            if (on.channelAsserted != undefined)
                on.channelAsserted({ channel });
            this.assertQueue({
                channel, queue
            }, ({ channel, q }) => {
                if (on.queueAsserted != undefined)
                    on.queueAsserted({ channel, q })
                channel.consume(q.queue, (msg) => {
                    consumer({ channel, msg })
                })
            })

        })
    }

    /**
     * Quick function to publish message to Exchange channel
     * @param {*} option Params to assert to channel
     *  - (string) queue: Name of queue
     *  - (string) type: type of Exchange, default is "fanout"
     * @param {*} msg  payload to publish to channel
     * @returns 
     */
    publishToExchange = ({ queue, type = 'fanout' }, msg) => {
        if (this.ampq == null) throw ('Connection to Amqp was not initialed')
        return this.ampq.createChannel().then((ch) => {
            var ok = ch.assertExchange(queue, type);
            return ok.then((_qok) => {
                ch.publish(queue, '', Buffer.from(msg));
                console.log(" [x] Sent '%s'", msg);
                return ch.close();
            });
        }).finally(() => this.ampq.close());
    }

    /**
     *  Assert to RabbitMQ Channel
     * @param {object} option Params to Assert Channel 
     * @param {function} onReady Callback function will be fire when asserted to channel
     * @returns 
     */
    assertExchange = ({ queue, type = 'fanout' }, onReady = () => { }) => {
        if (this.ampq == null) throw ('Connection to Amqp was not initialed')
        return this.ampq.createChannel().then(channel => {
            return channel.assertExchange(queue, type,
                // { durable: false, autoDelete: true }
            ).then(() => {
                return channel.assertQueue('', { exclusive: true }).then(q => {
                    channel.bindQueue(q.queue, queue, '')
                    return onReady({ channel, q })
                })

            })
        })
    }

    /**
     * Assert to RabbitMQ Queue
     * @param {object} option  Params to Assert Channel 
     * @param {*} onReady  Callback function will be fire when asserted to Queue
     * @returns 
     */
    assertQueue = ({ channel, queue }, onReady = () => { }) => {
        if (channel == undefined) console.error('[%s] %s %s', 'Error', 'assertQueue', 'Channel was not initialed')
        return channel.assertQueue('', { exclusive: true }).then(q => {
            channel.bindQueue(q.queue, queue, '')
            return onReady({ channel, q })
        })
    }

}

module.exports = RabbitmqWrapper;
