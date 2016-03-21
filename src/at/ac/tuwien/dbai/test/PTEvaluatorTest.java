import at.ac.tuwien.dbai.db.DBMetaData;
import at.ac.tuwien.dbai.opteval.PTXmlHandler;
import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PTEvaluatorTest {

    @org.junit.Test
    public void testMain() throws Exception {
        XMLReader parser = XMLReaderFactory.createXMLReader();

        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        parser.parse("resources/test.xml");

        EvalPT pt = handler.getEvalPT();
        MaxMappingSet mappingsIter = testEval(EvalPT.EvaluationType.ITERATIVE, null, pt);
        System.out.println("Finished evaluate ITERATIVE");
        for (DBMetaData.DBType type : DBMetaData.DBType.values()) {
            MaxMappingSet mappingsDB = testEval(EvalPT.EvaluationType.DB, type, pt);
            System.out.println("Finished evaluate DB - " + type.toString());
            assertEquals(mappingsDB.size(), mappingsIter.size());
            assertTrue(mappingsIter.containsAll(mappingsDB));
        }
    }

    private MaxMappingSet testEval(EvalPT.EvaluationType evalType, DBMetaData.DBType type, EvalPT pt) {
        MaxMappingSet mappingsDB = new MaxMappingSet();
        MappingSet setDB = pt.evaluate(evalType, type);
        mappingsDB.addAll(setDB);
        return mappingsDB;
    }
}