package com.example.dong23;

import android.os.Bundle;
import android.os.Handler;
import android.os.Message;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import static com.example.dong23.R.id.publish;
import static com.example.dong23.R.id.publish2;
import static com.example.dong23.R.id.publish3;

public class MainActivity extends AppCompatActivity {

    //    一个发送消息的生产者是一个用户程序。
    //    一个存储消息的队列是一个缓冲。
    //    一个接收消息的消费者是一个用户程序。
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        //服务器的连接：
        setupConnectionFactory();
        //发送消息的生产者
        publishToAMQP();
        setupPubButton();

        //用于从线程中获取数据，更新ui
        final Handler incomingMessageHandler = new Handler(){
            @Override
            public void handleMessage(Message msg){
                String message = msg.getData().getString("msg");
                String routingKey02 = msg.getData().getString("routingKey002");
                String routingKey03 = msg.getData().getString("routingKey003");

                TextView tv = (TextView) findViewById(R.id.textView);
                TextView tv2 = (TextView) findViewById(R.id.textView2);
                TextView tv3 = (TextView) findViewById(R.id.textView3);
                Date now = new Date();
                SimpleDateFormat ft = new SimpleDateFormat("hh.mm.ss");

                tv.append(ft.format(now) + ' ' + message + '\n');
                tv2.append(ft.format(now) + ' ' + routingKey02 + '\n');
                tv3.append(ft.format(now) + ' ' + routingKey03 + '\n');
                Log.i("test","msg = " + message);
            }
        };
        //开启消费者线程   接收消息的消费者. 接收者将会输出从RabbitMQ中获取到来自发送者的消息。接收者会一直保持运行，等待消息
        subscribe(incomingMessageHandler);
    }

    void setupPubButton(){
        Button button = (Button)findViewById(publish);
        button.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                EditText et = (EditText)findViewById(R.id.text);
                publishMessage(et.getText().toString());
                et.setText("");
            }
        });
        Button button2 = (Button)findViewById(publish2);
        button2.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                EditText et = (EditText)findViewById(R.id.text2);
                publishMessage2(et.getText().toString());
                et.setText("");
            }
        });
        Button button3 = (Button)findViewById(publish3);
        button3.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View arg0) {
                EditText et = (EditText)findViewById(R.id.text3);
                publishMessage3(et.getText().toString());
                et.setText("");
            }
        });
    }

    Thread subscribeThread;
    Thread publishThread;

    @Override
    protected void onDestroy(){
        super.onDestroy();
        publishThread.interrupt();
        subscribeThread.interrupt();
    }

    private BlockingDeque<String> queue = new LinkedBlockingDeque<String>();

    void publishMessage(String message){
        try{
            Log.d("","[q ] " + message);
            //putLast在这个队列的末尾插入指定元素，如果空间成为提供必要的等待。
            queue.putLast(message);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    private BlockingDeque<String> routingkey02 = new LinkedBlockingDeque<String>();

    void publishMessage2(String routing_key02){
        try{
            Log.d("","[q ] " + routing_key02);
            routingkey02.putLast(routing_key02);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    private BlockingDeque<String> routingkey03 = new LinkedBlockingDeque<String>();

    void publishMessage3(String routing_key03){
        try{
            Log.d("","[q ] " + routing_key03);
            routingkey03.putLast(routing_key03);
        }
        catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * 连接设置,我们创建一个服务器的连接：
     */
    //抽象的socket连接，注意协议版本的处理以及授权，诸如此类的事情。
    //这里我们连接到本地机器上的代理，因此它是localhost。如果我们想连接到不同机器上的代理，只需要说明它的主机名和IP地址。
    ConnectionFactory factory = new ConnectionFactory();
    private void setupConnectionFactory(){
        factory.setHost("192.168.1.199");
        factory.setPort(5672);
        factory.setUsername("test");
        factory.setPassword("test123");
    }

    /**
     * 生产者线程
     */
    //  生产者仅能将消息发送到一个交易所。一个交易所是一个非常简单的事物。
    // 在它的一遍，它从生产者那里接收消息，另一边将消息推送到队列中。
    // 这个交换所必须清楚的知道它所接收到的消息要如何处理。是否将它附加到一个特别的队列中？
    // 是否将它附加到多个队列中？或者是否它应该被丢弃。规则的定义是由交换类型决定的。
    public void publishToAMQP()
    {
        publishThread = new Thread(new Runnable(){
            //为队列命名
            private static final String EXCHANGE_NAME = "topic_logs";
            @Override
            public void run(){
                while(true){
                    try{
                        // 获取到连接以及mq通道
                        Connection connection = factory.newConnection();
                        Channel channel = connection.createChannel();

                        channel.confirmSelect();

                        //创建一个这种类型的交易所.我们必须声明一个发送队列，然后我们把消息发送到这个队列上
                        channel.exchangeDeclare(EXCHANGE_NAME,"topic");
                        // 路由关键字,消息通过路由关键字的名字路由到特定的队列上。
//                        String[] routing_keys = new String[] { "kernal.info", "cron.warning",
//                                "auth.info", "kernel.critical" };

                        while(true){
                            //输入关键字
//                            声明一个队列是幂等的，仅仅在要声明的队列不存在时才创建。消息内容是二进制数组，所以你可以随你喜好编码。
                            //takeFirst检索并移除此队列的第一个元素，如果有必要，直到一个元素可用等。
                            //takeLast检索并移除此队列的最后一个元素，如果有必要，直到一个元素可用等。
                            String message = queue.takeFirst();
                            String routing_key = routingkey02.takeFirst();

                            try{
//                                for (String routing_key : routing_keys) {
                                //这第一个参数是交易所的名字。空字符串说明它是默认的或者匿名的交易所：路由关键字存在的话，消息通过路由关键字的名字路由到特定的队列上。
                                channel.basicPublish(EXCHANGE_NAME, routing_key, null, message.getBytes());
                                System.out.println(" [x] Sent routingKey = " + routing_key + " ,msg = " + message + ".");
                                Log.d("", "[s] " + message);
                                channel.waitForConfirmsOrDie();
//                                }

                            }
                            catch(Exception e){
                                Log.d("","[f] "+ message);
                                //putFirst在这个队列的前面插入指定元素，如果空间成为提供必要的等待。
                                queue.putFirst(message);
                                routingkey02.putFirst(routing_key);
                                throw e;
                            }
                        }
                    }catch(InterruptedException e){
                        break;
                    }catch(Exception e){
                        Log.d("","Connection broken:" + e.getClass().getName());
                        try{
                            Thread.sleep(5000);
                        }catch(InterruptedException e1){
                            break;
                        }
                    }
                }
            }
        });
        publishThread.start();
    }

    /**
     * 消费者线程
     */
    //跟创建发送者相同，我们打开一个连接和一个通道，声明一个我们要消费的队列。注意要与发送的队列相匹配。
    void subscribe(final Handler handler){
        subscribeThread = new Thread(new Runnable(){
            private static final String EXCHANGE_NAME = "topic_logs";
            @Override
            public void run(){
                while(true){
                    try {
                        //创建一个连接   创建一个频道
                        Connection connection = factory.newConnection();
                        Channel channel = connection.createChannel();

                        //同一时刻服务器只会发一条消息给消费者 ,一次只发送一个，处理完成一个再获取下一个
                        channel.basicQos(1);

                        // 声明转发器,声明队列，主要为了防止消息接收者先运行此程序，队列还不存在时创建队列。
                        channel.exchangeDeclare(EXCHANGE_NAME,"topic");
                        // 队列名中包含一个随机队列名。使用无参数调用queueDeclare()方法，我们创建一个自动产生的名字，不持久化，独占的，自动删除的队列。
                        // 队列名中包含一个随机队列名。例如名字像amq.gen-JzTY20BRgKO-HjmUJj0wLg。
                        String queueName = channel.queueDeclare().getQueue();

                        //输入关键字
                        // 声明一个队列是幂等的，仅仅在要声明的队列不存在时才创建。消息内容是二进制数组，所以你可以随你喜好编码。
                        //takeFirst检索并移除此队列的第一个元素，如果有必要，直到一个元素可用等。
                        String routingKey03 = routingkey03.takeFirst();
                        //我们需要告诉交易所发送消息给我们的队列上。这交易所和队列之间的关系称之为一个绑定。
                        //接收所有routingKey相关的消息,将队列绑定到消息交换机exchange上
                        //一个绑定是一个交换所和一个队列之间的关系。这个很容易理解为：这个队列是对这交易所的消息感兴趣。
                        channel.queueBind(queueName, EXCHANGE_NAME, routingKey03);
                        System.out.println(" [#] Waiting for messages about routingKey. To exit press CTRL+C");


                        //注意我们在这里同样声明了一个队列。以为我们可能在发送者之前启动接收者，在我们从中获取消息之前我们想要确定这队列是否真实存在。
                        // 我们通知服务器通过此队列给我们发送消息。因此服务器会异步的给我们推送消息，在这里我们提供一个回调对象用来缓存消息，
                        // 直到我们准备好再使用它们。这就是QueueingConsumer所做的事。
                        QueueingConsumer consumer = new QueueingConsumer(channel);
                        // 监听队列，自动返回完成  false手动
                        channel.basicConsume(queueName,true,consumer);

                        //循环获取消息
                        while(true){
                                //获取消息，如果没有消息，这一步将会一直阻塞，开启nextDelivery阻塞方法（内部实现其实是阻塞队列的take方法）
                                QueueingConsumer.Delivery delivery = consumer.nextDelivery();
                                String message = new String(delivery.getBody());
                                String routingKey02  = delivery.getEnvelope().getRoutingKey();
                                System.out.println(" [x] Received routingKey = " + routingKey03 + ",msg = " + message + ".");
                                Log.d("", "[r] " + message);

                                //从message池中获取msg对象更高效
                                //对于Message对象，一般并不推荐直接使用它的构造方法得到，而是建议通过使用Message.obtain()这个静态的方法或者 Handler.obtainMessage()获取。
                                // Message.obtain()会从消息池中获取一个Message对象，如果消息池中是空的， 才会使用构造方法实例化一个新Message，这样有利于消息资源的利用。
                                //并不需要担心消息池中的消息过多，它是有上限的，上限为10个。 Handler.obtainMessage()具有多个重载方法，如果查看源码，会发现其实Handler.obtainMessage()在内部也是 调用的Message.obtain()。
                                Message msg = handler.obtainMessage();
                                Message routingKey003 = handler.obtainMessage();
                                Message routingKey002 = handler.obtainMessage();
                                Bundle bundle = new Bundle();
                                bundle.putString("msg", message);
                                bundle.putString("routingKey003", routingKey03);
                                bundle.putString("routingKey002", routingKey02);
                                msg.setData(bundle);
                                routingKey003.setData(bundle);
                                routingKey002.setData(bundle);
                                handler.sendMessage(msg);

                                //putFirst在这个队列的前面插入指定元素，如果空间成为提供必要的等待。
                                routingkey03.putFirst(routingKey03);
                        }
                    }catch(InterruptedException e){
                        break;
                    }catch (Exception el){
                        Log.d("","Connection broken: " + el.getClass().getName());
                        try{
                            Thread.sleep(4000);
                        }catch(InterruptedException e){
                            break;
                        }
                    }
                }
            }
        });
        subscribeThread.start();
    }

}





