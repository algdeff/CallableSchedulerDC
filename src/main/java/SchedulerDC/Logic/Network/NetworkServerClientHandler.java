package SchedulerDC.Logic.Network;

import static SchedulerDC.Facade.*;
import SchedulerDC.Publisher.Interfaces.ISubscriber;
import SchedulerDC.Publisher.Interfaces.IPublisherEvent;
import SchedulerDC.Publisher.Publisher;
import SchedulerDC.Publisher.PublisherEvent;

import java.io.*;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class NetworkServerClientHandler implements ISubscriber, Runnable {

    private String _clientID;

    private volatile boolean _sessionActive = true;

    private ThreadGroup _clientHandlerThreads;

    private BlockingQueue<PublisherEvent> _outcomeEventsToClientQueue;

    private AsynchronousSocketChannel _serverSocketChanhel;
    private ObjectInputStream _objectInputStream;
    private ObjectOutputStream _objectOutputStream;

    public NetworkServerClientHandler(AsynchronousSocketChannel socketChanhel, String clientID) {
        _clientID = clientID;
        _serverSocketChanhel = socketChanhel;
        _clientHandlerThreads = new ThreadGroup("clientHandlerThreads");
        _outcomeEventsToClientQueue = new LinkedBlockingQueue<>(50);
    }

    @Override
    public void run() {
        clientHandle();
    }

    private void clientHandle() {
        SocketAddress clientAddress = null;
        try {
            clientAddress = _serverSocketChanhel.getRemoteAddress();
            if (_serverSocketChanhel.isOpen()) {
                message("New client (" + _clientID + ") connected: " + clientAddress);

                InputStream inputStream = Channels.newInputStream(_serverSocketChanhel);
                _objectInputStream = new ObjectInputStream(inputStream);
                initOutcomeEventsToClientQueue();

                registerOnPublisher();

                sendEventToClient(new PublisherEvent(CMD_SERVER_SET_CLIENT_ID, _clientID).toServerCommand());

                while (_sessionActive) {
                    Object receivedObject = _objectInputStream.readObject();

                    if (!receivedObject.getClass().getName().equals(PublisherEvent.class.getName())) {
                        message("Incorrect event object type");
                        continue;
                    }
                    PublisherEvent eventFromClient = (PublisherEvent) receivedObject;

                    if (eventFromClient.getServerCommand() == null) {
                        message("No server command found in event: " + eventFromClient.getInterestName());
                        continue;
                    }

                    if (eventFromClient.getServerCommand().equals(CMD_SERVER_TERMINATE)) {
                        clientShutdown();
                        break;
                    }
                    parseCommandFromClient(eventFromClient);
                }

                message("Client (" + clientAddress + ") successfully terminated");
            }
        } catch (ClassNotFoundException e) {
            toLog(e.getMessage());
        } catch (IOException e) {
            message("Client ("+ clientAddress +") is breakdown!");
        } finally {
            clientShutdown();
        }

    }

    private void parseCommandFromClient(PublisherEvent eventFromClient) {

        switch (eventFromClient.getServerCommand()) {
            case CMD_SERVER_TRANSITION_EVENT: {
                publishTransitionEventFromClient(eventFromClient);
                return;
            }

        }
        message("Incorrect client command: " + eventFromClient.getServerCommand());

    }

    private void sendEventToClient(PublisherEvent publisherEvent) {  //synchronized if no queue
        try {
            _outcomeEventsToClientQueue.put(publisherEvent);
        } catch (InterruptedException e) {
            messageAndLog("sendEventToClient interrupted");
            toLog(e.getMessage());
            clientShutdown();
        }

    }

    private void initOutcomeEventsToClientQueue() {
        Thread outcomeQueueThread = new Thread(_clientHandlerThreads, () -> {
            OutputStream outputStream = Channels.newOutputStream(_serverSocketChanhel);
            try {
                _objectOutputStream = new ObjectOutputStream(outputStream);

                while (_sessionActive) {
                    PublisherEvent publisherEvent = _outcomeEventsToClientQueue.take();
                    _objectOutputStream.writeObject(publisherEvent);
                }

            } catch (InterruptedException | IOException e) {
                toLog("Output stream break!");
            } finally {
                clientShutdown();
            }

        }, "outcomeEventsToClientQueue");
        outcomeQueueThread.start();

    }

    private void clientShutdown() {
        if (!_sessionActive) {
            return;
        }
        _sessionActive = false;

        try {
            _objectOutputStream.flush();
            _serverSocketChanhel.shutdownOutput();
            _serverSocketChanhel.shutdownInput();
            _serverSocketChanhel.close();
        } catch (IOException e) {
            toLog("Client shutdown...IOException");
        } finally {
            message("CLIENT (" + _clientID + ") SHUTDOWN...");
            Publisher.getInstance().unregisterRemoteUser(_clientID);
            Publisher.getInstance().sendPublisherEvent(CMD_INTERNAL_CLIENT_BREAKDOWN, _clientID);
            _clientHandlerThreads.interrupt();
        }

    }


    //=============================================================================================

    private void publishTransitionEventFromClient(PublisherEvent eventFromClient) {
        Publisher.getInstance().sendPublisherEvent(eventFromClient);

    }

    private void sendMessageToClient(String message, String ClientID) {
        Publisher.getInstance().sendTransitionEvent(new PublisherEvent(
                CMD_LOGGER_CONSOLE_MESSAGE, "[SERVER] " + message), ClientID);
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
        Publisher.getInstance().registerRemoteUser(this, _clientID, TRANSITION_EVENT_GROUP_ALL_USERS);
    }

    @Override
    public String[] subscriberInterests() {
        return new String[0];
    }

    @Override
    public void listenerHandler(IPublisherEvent outcomeTransitionEvent) {
        if (outcomeTransitionEvent.getServerCommand().equals(CMD_SERVER_TRANSITION_EVENT)) {
            message("Send transition event to client: " + _clientID);
            sendEventToClient((PublisherEvent) outcomeTransitionEvent);
            return;
        }

    }

}
