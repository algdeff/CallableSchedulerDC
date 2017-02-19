package SchedulerDC.Logic;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.concurrent.ConcurrentHashMap;

public final class ConfigManager {

    /**
     ConfigManager

     Property keys from config file
     default: schedulerdc.conf.xml
     */

    private static final String           CONFIG_FILE_PATH  = "schedulerdc.conf.xml",

                                         REMOTE_SERVER_URL  =  "remote_server_url",
                                        REMOTE_SERVER_PORT  =  "remote_server_port",
                                             LOG_FILE_PATH  =  "log_file_path_name",
                                          THREAD_POOL_SIZE  =  "thread_pool_size",
                                       STATS_WRITE_INTERVAL  =  "stat_write_interval_sec",
                                      OUTCOMING_TYPES_GLOB  =  "outcoming_file_type_glob";

    private static final ConcurrentHashMap<String, String> _properties;

    static {
        _properties = new ConcurrentHashMap<>();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document;

            ClassLoader classLoader = ConfigManager.class.getClassLoader();
            File configFile = new File(CONFIG_FILE_PATH);
            if (configFile.exists()) {
                document = builder.parse(configFile);
            } else {
                document = builder.parse(classLoader.getResourceAsStream(CONFIG_FILE_PATH));
            }

            Element config = document.getDocumentElement();
            config.normalize();
            NodeList properties = config.getChildNodes();

            for (int i = 0; i < properties.getLength(); i++) {
                Node property = properties.item(i);
                if (property.getNodeType() == Node.ELEMENT_NODE) {
                    String propertyName = property.getNodeName();
                    String value = property.getTextContent();
                    System.err.println(propertyName + ": " + value);
                    _properties.put(propertyName, value);
                }

            }

        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
        }

    }

    private ConfigManager() {
    }

    public static void init() {
    }

    public static String getRemoteServerURL() {
        return _properties.get(REMOTE_SERVER_URL);
    }

    public static int getRemoteServerPort() {
        return Integer.parseInt(_properties.get(REMOTE_SERVER_PORT));
    }

    public static Path getLogFilePath() {
        return Paths.get(_properties.get(LOG_FILE_PATH));
    }

    public static int getStatsWriteInterval() {
        return Integer.parseInt(_properties.get(STATS_WRITE_INTERVAL));
    }

    public static int getThreadPoolSize() {
        return Integer.parseInt(_properties.get(THREAD_POOL_SIZE));
    }

    public static String getOutcomingTypesGlob() {
        return _properties.get(OUTCOMING_TYPES_GLOB);
    }

}