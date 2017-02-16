package SchedulerDC;

import static SchedulerDC.Facade.*;
import SchedulerDC.Logic.*;
import SchedulerDC.Logic.DistributedComputing.ClientTaskExecutor;
import SchedulerDC.Logic.DistributedComputing.ServerTaskProducer;
import SchedulerDC.Logic.ModeSelector;
import SchedulerDC.Logic.Workers.LogFileWorker;
import SchedulerDC.Publisher.Publisher;

/**
 * SchedulerDC
 *
 * Инициализация и завершение работы модулей
 *
 *
 * @author  Anton Butenko
 *
 */

public class ServerStarter {

    private static String _serverName = "SchedulerDC";
    private static ServerStarter _instance;

    private ModeSelector _process;

    private ServerStarter() {
    }

    public static void main(String[] args) {

        try {
            _instance = new ServerStarter();
            _instance.start();

        } catch (Throwable ex) {
            Publisher.getInstance().sendPublisherEvent(CMD_LOGGER_ADD_LOG,
                    "Failed start server:" + ex.getMessage());
            ex.printStackTrace(System.err);
            stopServerInstance();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(ServerStarter::stopServerInstance));
    }


    private void start() {

        Facade.getInstance().init();
        ConfigManager.init();
        ThreadPoolManager.getInstance()
                .init(ConfigManager.getThreadPoolSize());
        LogFileWorker.getInstance().init();

        ServerTaskProducer serverTaskProducer = new ServerTaskProducer();
        serverTaskProducer.init();

        ModeSelector modeSelector = new ModeSelector();
        modeSelector.start();

        ClientTaskExecutor clientTaskExecutor = new ClientTaskExecutor();
        clientTaskExecutor.init();

    }

    private void stop() {
        Publisher.getInstance().sendPublisherEvent(GLOBAL_SHUTDOWN);
        System.err.println("Server STOP");
    }

    private static void stopServerInstance() {
        if (_instance != null) {
            final ServerStarter serverStarter = _instance;
            _instance = null;
            serverStarter.stop();
        }
    }

    public static void stopAndExit(int status) {
        stopServerInstance();
        System.exit(status);
    }

    public static String getServerName() {
        return _serverName;
    }
}
