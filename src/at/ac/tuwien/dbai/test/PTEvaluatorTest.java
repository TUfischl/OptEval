package at.ac.tuwien.dbai.test;

import at.ac.tuwien.dbai.opteval.PTXmlHandler;
import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import at.ac.tuwien.dbai.sparql.query.MaxMappingSet;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.PrintStream;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Created by michael on 18.03.16.
 */
public class PTEvaluatorTest {

    @org.junit.Test
    public void testMain() throws Exception {
        XMLReader parser = XMLReaderFactory.createXMLReader();

        PTXmlHandler handler = new PTXmlHandler();
        parser.setContentHandler(handler);
        parser.parse("resources/test.xml");

        EvalPT pt = handler.getEvalPT();

        MaxMappingSet mappingsDB = new MaxMappingSet();
        MappingSet setDB = pt.evaluate(EvalPT.EvaluationType.DB);
        mappingsDB.addAll(setDB);
        System.out.println("Finished evaluate DB");

        MaxMappingSet mappingsIter = new MaxMappingSet();
        MappingSet setIter = pt.evaluate(EvalPT.EvaluationType.ITERATIVE);
        mappingsIter.addAll(setIter);
        System.out.println("Finished evaluate ITERATIVE");

        assertEquals(mappingsDB.size(), mappingsIter.size());
        assertTrue(mappingsIter.containsAll(mappingsDB));
    }
}