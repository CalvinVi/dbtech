package de.htwberlin.jdbc;

/**
 * @author Ingo Classen
 */

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import de.htwberlin.exceptions.*;
import oracle.jdbc.OracleConnectionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.htwberlin.domain.Kunde;

/**
 * VersicherungJdbc
 */
public class VersicherungJdbc implements IVersicherungJdbc {
  private static final Logger L = LoggerFactory.getLogger(VersicherungJdbc.class);
  private Connection connection;

  @Override
  public void setConnection(Connection connection) {
    this.connection = connection;
  }

  @SuppressWarnings("unused")
  private Connection useConnection() {
    if (connection == null) {
      throw new DataException("Connection not set");
    }
    return connection;
  }

  @Override
  public List<String> kurzBezProdukte() {
    L.info("start");
    List<String> kurzbezeichnungen = new ArrayList<String>();
    //TODO SQL-Query gegen die Datenbank ausführen
    String sqlString = "Select KurzBez FROM Produkt ORDER BY id ASC";
    try {
      // 3.Erzeugen eine Datenbankanweisung
      Statement s = useConnection().createStatement();
      //4.SQL Anweisung ausführen
      ResultSet r = s.executeQuery(sqlString);
      //5.Iteration durch die Ergebnismenge
      while(r.next()) {
        String kurzbez = r.getString("Kurzbez");
        kurzbezeichnungen.add(kurzbez);
        System.out.println(kurzbez);
      }
    }catch(SQLException e){
      //TODO Auto-generated catch block
      throw new DataException(e);
    }
    L.info("ende");
    return kurzbezeichnungen;
  }

  @Override
  public Kunde findKundeById(Integer id) {
    L.info("id: " + id);
    String sqlString = "SELECT * FROM Kunde WHERE id = ?";
    try {
      // Verbindung zur Datenbank herstellen
      Connection connection = useConnection();

      PreparedStatement statement = connection.prepareStatement(sqlString);

      // Wert für den Platzhalter setzen
      statement.setInt(1, id);

      // Abfrage ausführen
      ResultSet resultSet = statement.executeQuery();

      // Überprüfen, ob ein Ergebnis vorhanden ist
      if (resultSet.next()) {
        // Kundenobjekt erstellen und mit Daten aus der Datenbank füllen
        Kunde kunde = new Kunde();
        //System.out.println(resultSet.getInt("id"));
        //System.out.println(resultSet.getString("Name"));
        //System.out.println(resultSet.getDate("Geburtsdatum"));
        kunde.setId(resultSet.getInt("id"));
        kunde.setName(resultSet.getString("name"));
        kunde.setGeburtsdatum(resultSet.getDate("geburtsdatum").toLocalDate());

        System.out.println(kunde);

        L.info("Ende");
        return kunde;
      } else {
        // Wenn kein Kunde mit dieser ID gefunden wurde, Exception werfen
        throw new KundeExistiertNichtException("Kunde mit ID " + id + " existiert nicht.");
      }
    } catch (SQLException e) {
      // Bei SQL-Fehlern Exception werfen
      throw new DataException(e);
    }
  }

  @Override
  public void createVertrag(Integer id, Integer produktId, Integer kundenId, LocalDate versicherungsbeginn) throws VertragExistiertBereitsException {
    L.info("id: " + id);
    L.info("produktId: " + produktId);
    L.info("kundenId: " + kundenId);
    L.info("versicherungsbeginn: " + versicherungsbeginn);
    // Berechnen des Versicherungsendes (1 Jahr minus 1 Tag nach dem Versicherungsbeginn)
    LocalDate versicherungsende = versicherungsbeginn.plusYears(1).minusDays(1);
    if (versicherungsbeginn.isBefore(LocalDate.now())) {
      throw new DatumInVergangenheitException(versicherungsbeginn);
    }
    if (vertragExistiertBereits(id)) {
      throw new VertragExistiertBereitsException(id);
    }
    if (!produktExistiert(produktId)) {
      throw new ProduktExistiertNichtException(produktId);
    }
    {
      if (!kundeExistiert(kundenId)) {
        throw new KundeExistiertNichtException("Der kunde existiert nicht");
      }
    try {
      // SQL-Anweisung zum Einfügen des Vertrags in die Datenbank
      String sqlString = "INSERT INTO Vertrag (id, Produkt_FK, Kunde_FK, versicherungsbeginn, versicherungsende) " +
              "VALUES (?, ?, ?, ?, ?)";
      Connection connection = useConnection();
      PreparedStatement statement = connection.prepareStatement(sqlString);
      statement.setInt(1, id);
      statement.setInt(2, produktId);
      statement.setInt(3, kundenId);
      statement.setDate(4, Date.valueOf(versicherungsbeginn));
      statement.setDate(5, Date.valueOf(versicherungsende));

      // Einfügen des Vertrags in die Datenbank
      statement.executeUpdate();

      L.info("Ende");
    }catch (SQLException e) {
      e.printStackTrace();

      }
    }
  }

  private boolean vertragExistiertBereits(Integer id) throws VertragExistiertBereitsException {
    String sql = "SELECT ID FROM vertrag WHERE id = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, id);
      try (ResultSet resultSet = statement.executeQuery()) {
          return resultSet.next();
      }
    }
   catch (SQLException e) {
    e.printStackTrace();
  }
    return false;
}

  private boolean produktExistiert(Integer produktId) {
    String sql = "SELECT ID FROM Produkt WHERE ID = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, produktId);
      try (ResultSet resultSet = statement.executeQuery()) {
        return resultSet.next();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }

  private boolean kundeExistiert(Integer kundenId) {
    String sql = "SELECT ID FROM Kunde WHERE ID = ?";
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setInt(1, kundenId);
      try (ResultSet resultSet = statement.executeQuery()) {
          return resultSet.next();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return false;
  }


  @Override
  public BigDecimal calcMonatsrate(Integer vertragsId) throws VertragExistiertNichtException{
    L.info("vertragsId: " + vertragsId);
    BigDecimal monatsrate = BigDecimal.ZERO;
    try {String sql="SELECT SUM(dp.Preis) AS Monatsrate FROM Vertrag v " +
            "JOIN Deckung d ON v.ID = d.Vertrag_FK " +
            "JOIN Deckungsart da ON d.Deckungsart_FK = da.ID " +
            "JOIN Deckungsbetrag db ON da.ID = db.Deckungsart_FK " +
            "JOIN Deckungspreis dp ON da.ID = dp.Deckungsbetrag_FK " +
            "WHERE v.ID = ? " +
            "AND v.Versicherungsbeginn BETWEEN dp.Gueltig_Von AND dp.Gueltig_Bis";
    PreparedStatement statement = connection.prepareStatement(sql) ;
      statement.setInt(1, vertragsId);
      ResultSet resultSet = statement.executeQuery();

      if (resultSet.next()) {
        monatsrate = resultSet.getBigDecimal("Monatsrate");
        if (monatsrate == null) {
          monatsrate = BigDecimal.ZERO;
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      throw new VertragExistiertNichtException(vertragsId);
    }
    System.out.println(monatsrate);
    L.info("ende");
    return monatsrate;
  }
}
