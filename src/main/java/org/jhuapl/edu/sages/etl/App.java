package org.jhuapl.edu.sages.etl;

import java.io.IOException;

import org.jhuapl.edu.sages.etl.opencsvpods.DumbTestOpenCsvJar;

/**
 * Hello world!
 */
public class App {

    public static void main(String[] args) {
        try {
            DumbTestOpenCsvJar.main(args);
        } catch (SagesEtlException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
