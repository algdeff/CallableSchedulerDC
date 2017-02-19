package SchedulerDC.Logic;

import static SchedulerDC.Facade.*;

import SchedulerDC.Facade;
import SchedulerDC.Logic.Network.RemoteClient;
import SchedulerDC.Logic.Network.NetworkServer;
import SchedulerDC.Publisher.Publisher;
import SchedulerDC.ServerStarter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class ModeSelector {

    public ModeSelector() {
    }

    public void start() {

        boolean isSelected = false;

        while (!isSelected) {
            messageLog(    "   You choice: \n" +
                           "1. Start CLIENT role\n" +
                           "2. Start SERVER role\n" +
                           "3. Terminate the program\n");
            BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
            String choice = "";
            try {
                choice = stdin.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            switch (choice) {
                case "1": {
                    isSelected = true;
                    startClientRole();
                    break;
                }
                case "2": {
                    isSelected = true;
                    startServerRole();
                    break;
                }
                case "3": {
                }

                ServerStarter.stopAndExit(0);
            }

        }

    }

    private void startClientRole() {
        messageLog("StartClientRole");

        Facade.setServerRole(false);

        RemoteClient remoteClient = new RemoteClient(
                ConfigManager.getRemoteServerURL(),
                ConfigManager.getRemoteServerPort());
        remoteClient.start();

    }

    private void startServerRole() {
        messageLog("StartServerRole");

        Facade.setServerRole(true);

        NetworkServer networkServer = new NetworkServer(
                ConfigManager.getRemoteServerPort());
        networkServer.start();

    }

    private void messageLog(String message) {
        Publisher.getInstance().sendPublisherEvent(CMD_LOGGER_ADD_LOG, message);
    }

}

