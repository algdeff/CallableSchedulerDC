package SchedulerDC.Logic.Network;

import static SchedulerDC.Facade.*;
import SchedulerDC.Publisher.Interfaces.ISubscriber;
import SchedulerDC.Publisher.Interfaces.IPublisherEvent;
import SchedulerDC.Publisher.Publisher;
import SchedulerDC.Publisher.PublisherEvent;
import SchedulerDC.ServerStarter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import java.io.*;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class RemoteClient implements ISubscriber {

    private String _clientID = null;

    private volatile boolean _sessionActive = true;
    private boolean _uiActive = true;

    private boolean _skipClientMenu = false;

    private ThreadGroup _clientControllerThreads;

    private String _remoteServerUrl;
    private int _remoteServerPort;

    private BlockingQueue<PublisherEvent> _outcomeEventsToServerQueue;

    private AsynchronousSocketChannel _clientSocketChannel;
    private ObjectOutputStream _objectOutputStream;
    private ObjectInputStream _objectInputStream;


    public RemoteClient(String remoteServerUrl, int remoteServerPort) {
        _remoteServerUrl = remoteServerUrl;
        _remoteServerPort = remoteServerPort;
        _clientControllerThreads = new ThreadGroup("clientControllerThreads");
        _outcomeEventsToServerQueue = new LinkedBlockingQueue<>(50);
        _clientSocketChannel = null;
    }

    public void start() {
        openConnectionToServer();

    }

    private void openConnectionToServer() {

        try {
            SocketAddress hostAddress = new InetSocketAddress(InetAddress.getByName(_remoteServerUrl),_remoteServerPort);

            boolean isConnected = false;
            while (!isConnected) {
                _clientSocketChannel = AsynchronousSocketChannel.open();
                try {
                    _clientSocketChannel.connect(hostAddress).get();
                    isConnected = true;
                } catch (ExecutionException | InterruptedException ee) {
                    _clientSocketChannel.close();
                    message("Connecting to server: " + hostAddress + "......");
                    try {
                        TimeUnit.SECONDS.sleep(10);
                    } catch (InterruptedException e) {
                        toLog(e.getMessage());
                    }
                }
            }
            message("Connected to server " + hostAddress);

            initOutcomeEventsToServerQueue();

            registerOnPublisher();

            InputStream inputStream = Channels.newInputStream(_clientSocketChannel);
            _objectInputStream = new ObjectInputStream(inputStream);

            new Thread(new ServerEventMonitor()).start();
            new Thread(new ClientInterface()).start();

        } catch (IOException e) {
            message("Server breakdown!");
            clientShutdown();
        }

    }

    private void initOutcomeEventsToServerQueue() {

        Thread outcomeQueueThread = new Thread(_clientControllerThreads, () -> {
            OutputStream outputStream = Channels.newOutputStream(_clientSocketChannel);
            try {
                _objectOutputStream = new ObjectOutputStream(outputStream);

                while (_sessionActive) {
                    PublisherEvent publisherEvent = _outcomeEventsToServerQueue.take();
                    _objectOutputStream.writeObject(publisherEvent);
                }

            } catch (InterruptedException | IOException e) {
                message("Output stream break!");
            } finally {
                clientShutdown();
            }

        }, "outcomeEventsToServerQueue");
        outcomeQueueThread.start();

    }

    private void clientShutdown() {
        if (!_sessionActive) {
            return;
        }
        _sessionActive = false;

        try {
            _objectOutputStream.flush();
            _objectOutputStream.close();
        } catch (IOException e) {
            toLog("Client shutdown...IOException");
        } finally {
            message("CLIENT (" + _clientID + ") SHUTDOWN...");
        }

        ServerStarter.stopAndExit(1);

    }


    //=============================================================================================

    private class ServerEventMonitor implements Runnable {

        @Override
        public void run() {
            serverCommandListener();
        }

        private void serverCommandListener() {
            try {
                while (_sessionActive) {
                    Object receivedObject = _objectInputStream.readObject();

                    if (!receivedObject.getClass().getName().equals(PublisherEvent.class.getName())) {
                        message("Incorrect event object type");
                        continue;
                    }
                    PublisherEvent eventFromServer = (PublisherEvent) receivedObject;

                    if (eventFromServer.getServerCommand() == null) {
                        message("No server command found in event: " + eventFromServer.getInterestName());
                        continue;
                    }

                    if (eventFromServer.getServerCommand().equals(CMD_SERVER_TERMINATE)) {
                        message("CMD_SERVER_TERMINATE");
                        clientShutdown();
                        break;
                    }
                    parseCommandFromServer(eventFromServer);
                }

            } catch (IOException | ClassNotFoundException e) {
                message("serverCommandListener interrupted: " + e.getMessage());
                clientShutdown();
            }

        }

        private void parseCommandFromServer(PublisherEvent eventFromServer) {

            switch (eventFromServer.getServerCommand()) {
                case CMD_SERVER_SET_CLIENT_ID: {
                    setClientID((String) eventFromServer.getBody());
                    toLog(getClientID());
                    return;
                }
                case CMD_SERVER_TRANSITION_EVENT: {
                    publishTransitionEventFromServer(eventFromServer);
                    return;
                }

            }
            message("Incorrect server command: " + eventFromServer.getServerCommand());

        }

    }

    private void sendEventToServer(PublisherEvent publisherEvent) {  //synchronyzed if no queue
        try {
            _outcomeEventsToServerQueue.put(publisherEvent);
        } catch (InterruptedException e) {
            messageAndLog("sendEventToServer interrupted");
            toLog(e.getMessage());
            clientShutdown();
        }

    }

    private void publishTransitionEventFromServer(PublisherEvent eventFromServer) {
        Publisher.getInstance().sendPublisherEvent(eventFromServer);
    }


    //=============================================================================================

    private void showClientMenu() {
        if (_skipClientMenu) {
            _skipClientMenu = false;
            return;
        }

        message( "   You choice: \n" +
                "1. Run task factory\n" +
                "2. Terminate the program\n");
    }

    private class ClientInterface implements  Runnable {

        @Override
        public void run() {
            clientCommandListener();
        }

        private void clientCommandListener() {
            try {

                showClientMenu();

                while (_uiActive && _sessionActive) {

                    BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
                    String choice = stdin.readLine();

                    switch (choice) {

                        case "1": {
                            runTaskExecutor();
                            break;
                        }
                        case "2": {
                        }
                        clientShutdown();
                    }

                }
            } catch (IOException e) {
                toLog(e.getMessage());
            }

            message("/Client UI terminate/");

        }

        private void runTaskExecutor() {
            Publisher.getInstance().sendPublisherEvent(CMD_TASK_EXECUTOR_START, getClientID());

        }

    }


    //=============================================================================================

    private void setClientID(String clientID) {
        if (_clientID != null) return;
        _clientID = clientID;
    }
    private String getClientID() {
        return _clientID;
    }

    private void skipClientMenu() {
        _skipClientMenu = true;
    }

    private void message(String message) {
        Publisher.getInstance().sendPublisherEvent(CMD_LOGGER_CONSOLE_MESSAGE, message);
    }

    private void messageAndLog(String message) {
        Publisher.getInstance().sendPublisherEvent(CMD_LOGGER_ADD_LOG, message);
    }

    private void toLog(String message) {
        Publisher.getInstance().sendPublisherEvent(CMD_LOGGER_ADD_RECORD, message);
    }


    //=============================================================================================

    @Override
    public void registerOnPublisher() {
        Publisher.getInstance().registerNewSubscriber(this, TRANSITION_EVENT_GROUP_CLIENT);
    }

    @Override
    public String[] subscriberInterests() {
        return new String[] {
                CMD_NET_CLIENT_UI_BREAK,
                GLOBAL_SHUTDOWN
        };
    }

    @Override
    public void listenerHandler(IPublisherEvent publisherEvent) {
        if (publisherEvent.getServerCommand().equals(CMD_SERVER_TRANSITION_EVENT)) {
            toLog("Send transition event to Server, type is: " + publisherEvent.getGroupName());
            sendEventToServer((PublisherEvent) publisherEvent);
            return;
        }
        if (publisherEvent.getType().equals(EVENT_TYPE_GROUP)) {
            message("NetClient received group event ("
                    + publisherEvent.getGroupName() + "): \n" + publisherEvent.getBody().toString());
        }

        switch (publisherEvent.getInterestName()) {
            case CMD_NET_CLIENT_UI_BREAK: {
                _uiActive = false;
                break;
            }

            case GLOBAL_SHUTDOWN: {
                clientShutdown();
                break;
            }

        }

    }

}