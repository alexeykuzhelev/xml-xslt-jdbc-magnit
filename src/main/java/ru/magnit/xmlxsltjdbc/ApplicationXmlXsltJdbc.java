package ru.magnit.xmlxsltjdbc;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.*;
import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.xpath.*;

import java.io.*;
import java.sql.*;

/**
 * Класс для работы с Базой данных и XML документами.
 * Задание:
 * - создать таблицу в БД, если отсутствует
 * - вставить в таблицу значения 1...N
 * - сформировать XML документ вида
 *   <entries>
 *     <entry>
 *       <field>значение поля field</field>
 *     </entry>
 *    ...
 *     <entry>
 *       <field>значение поля field</field>
 *     </entry>
 *   </entries>
 * сохранить документ под именем 1.xml
 * - посредством XSLT преобразовать документ к виду
 * <entries>
 *   <entry field="значение поля field">
 *   ...
 *   <entry field="значение поля field">
 * </entries>
 * (с N вложенных элементов <entry>)
 * сохранить документ под именем 2.xml
 * - парсить 2.xml  и вывести сумму значений всех атрибутов field в консоль
 */

public class ApplicationXmlXsltJdbc {

    private String user;
    private String pass;
    private int number;

    /**
     * Устанавливаем значения для подключения БД.
     * @param user пользователь
     * @param pass пароль
     */
    public void setCredentials(String user, String pass) {
        this.user = user;
        this.pass = pass;
    }

    /**
     * Устанавливаем значение количества записей в БД.
     * @param number количество записей
     */
    public void setNumber(int number) {
        this.number = number;
    }

    public void init() {
        Connection conn = null;
        try {
            //подключение к базе данных
            conn = this.connectDB();

            //создание таблицы
            this.createTable(conn);

            //заполнение таблицы данными
            this.insertValues(conn);

            //создание xml документа с данными БД
            this.makeXML(conn);

            //изменение стиля XML с использованием XSLT
            this.transformXML();

            //парсинг документа с использованием StAX
            this.parseXMLbyStAX();

            //парсинг документа с использованием xPath
            this.parseXMLbyXPath();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Метод подключения к базе данных.
     */
    private Connection connectDB() throws SQLException {
        String url = "jdbc:postgresql://localhost:5432/postgres";
        return DriverManager.getConnection(url, this.user, this.pass);
    }

    /**
     * Метод создания таблицы.
     * @param conn подключение к базе данных
     */
    private void createTable(Connection conn) {
        Statement st = null;

        try {
            st = conn.createStatement();
            String table = "CREATE TABLE IF NOT EXISTS numbers ("
                    + "id SERIAL PRIMARY KEY,"
                    + "num int NOT NULL"
                    + ");"
                    + "DELETE FROM numbers";
            st.executeUpdate(table);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Метод заполнения таблицы данными.
     * @param conn подключение к базе данных
     */
    private void insertValues(Connection conn) {
        PreparedStatement pst = null;
        try {
            pst = conn.prepareStatement("INSERT INTO numbers (num) VALUES (?)");
            conn.setAutoCommit(false);
            for (int i = 1; i <= this.number; i++) {
                pst.setInt(1, i);
                pst.addBatch();
            }
            pst.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            try {
                System.err.println("Transaction rollback");
                e.printStackTrace();
                conn.rollback();
            } catch (SQLException e1) {
                System.err.println("SQLException during rollback");
                e1.printStackTrace();
            }
        } finally {
            if (pst != null) {
                try {
                    pst.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Метод создания XML файла из данных БД.
     * @param conn подключение базы данных
     */
    private void makeXML(Connection conn) {
        String filePath = "src\\main\\resources\\1.xml";
        StringWriter stringWriter = new StringWriter();
        XMLOutputFactory xmlOutputFactory = XMLOutputFactory.newInstance();
        xmlOutputFactory.setProperty(XMLOutputFactory.IS_REPAIRING_NAMESPACES, Boolean.TRUE);
        Statement st = null;

        try {
            st = conn.createStatement();
            ResultSet rs = st.executeQuery("SELECT * FROM numbers");

            XMLStreamWriter xmlStreamWriter = xmlOutputFactory.createXMLStreamWriter(stringWriter);
            xmlStreamWriter.writeStartDocument("1.0");
            xmlStreamWriter.writeStartElement("entries");
                    while (rs.next()) {
                        xmlStreamWriter.writeStartElement("entry");
                        xmlStreamWriter.writeStartElement("field");
                        xmlStreamWriter.writeCharacters(Integer.toString(rs.getInt("num")));
                        xmlStreamWriter.writeEndElement();
                        xmlStreamWriter.writeEndElement();
                    }
            xmlStreamWriter.writeEndElement();
            xmlStreamWriter.writeEndDocument();
            xmlStreamWriter.flush();
            xmlStreamWriter.close();

            StreamSource inputXML = new StreamSource(new StringReader(stringWriter.getBuffer().toString()));
            StreamResult outputXML = new StreamResult(new FileOutputStream(filePath));
            Transformer transformer = TransformerFactory.newInstance().newTransformer();
            setOutputPropertyTransformer(transformer);
            transformer.transform(inputXML, outputXML);

            rs.close();

        } catch (SQLException | XMLStreamException | TransformerException | FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (st != null) {
                try {
                    st.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Метод преобразования XML документа с использованием XSLT.
     */
    private void transformXML() {
        String xml = "src\\main\\resources\\1.xml";
        String xsl = "src\\main\\resources\\stylesheet.xsl";
        String result = "src\\main\\resources\\2.xml";

        try {
            StreamSource inputXML = new StreamSource(new FileInputStream(xml));
            StreamSource inputXSL = new StreamSource(new FileInputStream(xsl));
            StreamResult outputXML = new StreamResult(new FileOutputStream(result));
            Transformer transformer = TransformerFactory.newInstance().newTransformer(inputXSL);
            setOutputPropertyTransformer(transformer);
            transformer.transform(inputXML, outputXML);
        } catch (TransformerException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод парсинга xml документа и вывода суммы всех значений с использованием StAX.
     */
    private void parseXMLbyStAX() {
        String filePath = "src\\main\\resources\\2.xml";
        XMLInputFactory xmInputFactory = XMLInputFactory.newInstance();

        try {
            XMLStreamReader xmlStreamReader = xmInputFactory.createXMLStreamReader(new FileInputStream(filePath));

            int sum = 0;
            while (xmlStreamReader.hasNext()) {
                xmlStreamReader.next();
                if (xmlStreamReader.isStartElement() && xmlStreamReader.getLocalName().equals("entry")) {
                    sum += Integer.valueOf(xmlStreamReader.getAttributeValue(0));
                }
            }
            System.out.println(String.format("StAX: Сумма всех значений %s", sum));

        } catch (XMLStreamException | FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * Метод парсинга xml документа и вывода суммы всех значений с использованием xPath.
     */
    private void parseXMLbyXPath() {
        String filePath = "src\\main\\resources\\2.xml";

        try {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(filePath);

        XPathFactory xPathFactory = XPathFactory.newInstance();
        XPath xpath = xPathFactory.newXPath();
        XPathExpression expression = xpath.compile("//entries/entry/@field");

        NodeList nodeList = (NodeList) expression.evaluate(doc, XPathConstants.NODESET);
        int sum = 0;
        for (int i = 0; i < nodeList.getLength(); i++) {
            Node node = nodeList.item(i);
            sum += Integer.valueOf(node.getTextContent());
        }
        System.out.println(String.format("xPath: Сумма всех значений %s", sum));

        } catch (ParserConfigurationException | SAXException | IOException | XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    private void setOutputPropertyTransformer(Transformer transformer) {
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty(OutputKeys.STANDALONE, "yes");
        transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        transformer.setOutputProperty(OutputKeys.METHOD, "xml");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    }

    public static void main(String[] args) {
        ApplicationXmlXsltJdbc app = new ApplicationXmlXsltJdbc();
        app.setCredentials("postgres", "aleks");
        app.setNumber(10);
        app.init();
    }
}
