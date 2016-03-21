import at.ac.tuwien.dbai.db.DBConnection;
import at.ac.tuwien.dbai.db.DBMetaData;
import at.ac.tuwien.dbai.opteval.PTXmlHandler;
import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import static org.junit.Assert.*;

public class PTEvaluatorTest {

    @org.junit.Test
    public void testMain() throws Exception {
        XMLReader parser = XMLReaderFactory.createXMLReader();

        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        parser.parse("resources/test.xml");

        EvalPT pt = handler.getEvalPT();
        MaxMappingSet mappingsIter = testEval(EvalPT.EvaluationType.ITERATIVE, pt);
        System.out.println("Finished evaluate ITERATIVE");
        for (DBMetaData.DBType type : DBMetaData.DBType.values()) {
            DBConnection.setMetaData(type);
            MaxMappingSet mappingsDB = testEval(EvalPT.EvaluationType.DB, pt);
            System.out.println("Finished evaluate DB - " + type.toString());
            assertEquals(mappingsDB.size(), mappingsIter.size());
            assertTrue(mappingsIter.containsAll(mappingsDB));
        }
    }

    private MaxMappingSet testEval(EvalPT.EvaluationType evalType, EvalPT pt) {
        MaxMappingSet mappingsDB = new MaxMappingSet();
        MappingSet setDB = pt.evaluate(evalType);
        mappingsDB.addAll(setDB);
        return mappingsDB;
    }
}