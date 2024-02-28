package gr.uoa.di.madgik.elastic;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver {

    private static Driver driver;

    private static final Logger logger = Logger.getLogger(Driver.class.getName());
    private static final String STARTS_WITH = "jdbc:elastic://";

    static {
        try {
            // Ensure Driver is registered only once.
            register();
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    /**
     * Create a new Elasticsearch {@link Connection connection}.
     *
     * @param url the URL of the database to which to connect
     * @param info a list of arbitrary string tag/value pairs as
     * connection arguments. Normally at least a "user" and
     * "password" property should be included.
     * @return
     * @throws SQLException
     */
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (url == null) {
            throw new SQLException("Url is null");
        } else if (url.startsWith(STARTS_WITH)) {
            return new JdbcConnection(url, info);
        } else {
            return null;
        }
    }

    /**
     * Checks whether the provided url is supported.
     *
     * @param url the URL of the database
     * @return
     * @throws SQLException
     */
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("Url is null");
        } else if (url.startsWith(STARTS_WITH)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return logger;
    }

    public static synchronized void register() throws SQLException {
        if (isRegistered()) {
            throw new IllegalStateException(
                    "Driver is already registered. It can only be registered once.");
        }
        Driver.driver = new Driver();
        DriverManager.registerDriver(Driver.driver);
    }

    public static synchronized void deregister() throws SQLException {
        if (!isRegistered()) {
            throw new IllegalStateException(
                    "Driver is not registered (or it has not been registered using Driver.register() method)");
        }
        DriverManager.deregisterDriver(Driver.driver);
        Driver.driver = null;
    }

    public static boolean isRegistered() {
        return driver != null;
    }
}
