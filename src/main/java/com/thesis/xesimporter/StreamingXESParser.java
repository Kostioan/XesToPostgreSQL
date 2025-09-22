package com.thesis.xesimporter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;
import java.io.FileInputStream;
import java.io.File;
import java.util.*;

/**
 * Streaming XES parser for large files - uses minimal memory
 */
public class StreamingXESParser {
    private static final Logger logger = LoggerFactory.getLogger(StreamingXESParser.class);

    public interface XESHandler {
        void onLogStart(String logName);
        void onExtension(String name, String prefix, String uri);
        void onClassifier(String name, String keys);
        void onGlobalAttributes(Map<String, String> traceGlobal, Map<String, String> eventGlobal);
        void onTrace(XESTrace trace);
        void onLogEnd();
    }

    public static class XESTrace {
        private final List<XESEvent> events = new ArrayList<>();
        private final Map<String, String> attributes = new HashMap<>();

        public List<XESEvent> getEvents() { return events; }
        public Map<String, String> getAttributes() { return attributes; }
        public void addEvent(XESEvent event) { events.add(event); }
        public void addAttribute(String key, String value) { attributes.put(key, value); }
    }

    public static class XESEvent {
        private final Map<String, String> attributes = new HashMap<>();

        public Map<String, String> getAttributes() { return attributes; }
        public void addAttribute(String key, String value) { attributes.put(key, value); }
    }

    public static void parseXESFileStreaming(File xesFile, XESHandler handler) throws Exception {
        logger.info("Starting streaming parse of XES file: {} ({} MB)",
                xesFile.getName(), xesFile.length() / (1024 * 1024));

        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_COALESCING, true);
        factory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, false);

        long tracesProcessed = 0;
        long startTime = System.currentTimeMillis();

        try (FileInputStream fis = new FileInputStream(xesFile)) {
            XMLStreamReader reader = factory.createXMLStreamReader(fis);

            String logName = null;
            Map<String, String> globalTraceAttributes = new HashMap<>();
            Map<String, String> globalEventAttributes = new HashMap<>();

            while (reader.hasNext()) {
                int event = reader.next();

                if (event == XMLStreamConstants.START_ELEMENT) {
                    String elementName = reader.getLocalName();

                    switch (elementName) {
                        case "log":
                            logName = reader.getAttributeValue(null, "concept:name");
                            if (logName == null) logName = "Unnamed Log";
                            handler.onLogStart(logName);
                            break;

                        case "extension":
                            String extName = reader.getAttributeValue(null, "name");
                            String extPrefix = reader.getAttributeValue(null, "prefix");
                            String extUri = reader.getAttributeValue(null, "uri");
                            handler.onExtension(extName, extPrefix, extUri);
                            break;

                        case "classifier":
                            String classifierName = reader.getAttributeValue(null, "name");
                            String classifierKeys = reader.getAttributeValue(null, "keys");
                            handler.onClassifier(classifierName, classifierKeys);
                            break;

                        case "global":
                            String scope = reader.getAttributeValue(null, "scope");
                            Map<String, String> globalAttrs = "trace".equals(scope) ?
                                    globalTraceAttributes : globalEventAttributes;
                            parseGlobalElement(reader, globalAttrs);
                            break;

                        case "trace":
                            XESTrace trace = parseTraceElement(reader);
                            handler.onTrace(trace);
                            tracesProcessed++;

                            if (tracesProcessed % 1000 == 0) {
                                long elapsed = System.currentTimeMillis() - startTime;
                                double rate = tracesProcessed / (elapsed / 1000.0);
                                logger.info("Processed {} traces (rate: {:.1f} traces/sec)",
                                        tracesProcessed, rate);
                            }
                            break;
                    }
                }
            }

            // Send global attributes once after parsing is complete
            if (!globalTraceAttributes.isEmpty() || !globalEventAttributes.isEmpty()) {
                handler.onGlobalAttributes(globalTraceAttributes, globalEventAttributes);
            }

            handler.onLogEnd();

            reader.close();
        }

        long totalTime = System.currentTimeMillis() - startTime;
        logger.info("Completed streaming parse: {} traces in {:.2f} seconds",
                tracesProcessed, totalTime / 1000.0);
    }

    private static XESTrace parseTraceElement(XMLStreamReader reader) throws Exception {
        XESTrace trace = new XESTrace();

        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                String elementName = reader.getLocalName();

                if ("event".equals(elementName)) {
                    XESEvent xesEvent = parseEventElement(reader);
                    trace.addEvent(xesEvent);
                } else if (isAttributeElement(elementName)) {
                    String key = reader.getAttributeValue(null, "key");
                    String value = reader.getAttributeValue(null, "value");
                    if (key != null) {
                        trace.addAttribute(key, value != null ? value : "");
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "trace".equals(reader.getLocalName())) {
                break;
            }
        }

        return trace;
    }

    private static XESEvent parseEventElement(XMLStreamReader reader) throws Exception {
        XESEvent event = new XESEvent();

        while (reader.hasNext()) {
            int eventType = reader.next();

            if (eventType == XMLStreamConstants.START_ELEMENT) {
                String elementName = reader.getLocalName();

                if (isAttributeElement(elementName)) {
                    String key = reader.getAttributeValue(null, "key");
                    String value = reader.getAttributeValue(null, "value");
                    if (key != null) {
                        event.addAttribute(key, value != null ? value : "");
                    }
                }
            } else if (eventType == XMLStreamConstants.END_ELEMENT && "event".equals(reader.getLocalName())) {
                break;
            }
        }

        return event;
    }

    private static void parseGlobalElement(XMLStreamReader reader, Map<String, String> globalAttrs) throws Exception {
        while (reader.hasNext()) {
            int event = reader.next();

            if (event == XMLStreamConstants.START_ELEMENT) {
                String elementName = reader.getLocalName();

                if (isAttributeElement(elementName)) {
                    String key = reader.getAttributeValue(null, "key");
                    String value = reader.getAttributeValue(null, "value");
                    if (key != null) {
                        globalAttrs.put(key, value != null ? value : "");
                    }
                }
            } else if (event == XMLStreamConstants.END_ELEMENT && "global".equals(reader.getLocalName())) {
                break;
            }
        }
    }

    private static boolean isAttributeElement(String elementName) {
        return elementName.equals("string") || elementName.equals("date") ||
                elementName.equals("int") || elementName.equals("float") ||
                elementName.equals("boolean") || elementName.equals("id");
    }
}