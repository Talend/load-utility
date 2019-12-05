package com.thoughtspot.load_utility;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    public static void main(String[] args)
    {
        try {
            TSReader reader = TSReader.newInstance(0);
            TSLoadUtility tsLoadUtility = TSLoadUtility.getInstance("ts.aws.com", 22, "admin",
                    "th0ughtSp0t");
            tsLoadUtility.connect();
            //LinkedHashMap<String, String> schema = tsLoadUtility.getTableColumns("test1",
             //       "falcon_default_schema", "TEST5");
            ExecutorService executorService = Executors.newSingleThreadExecutor();
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    tsLoadUtility.retrieve("test1",
                            "falcon_default_schema.TEST5", reader);
                    Thread.yield();
                }
            });



                String record = null;
                int x = 1;
                while(true)
                {
                    record = reader.poll();

                    if (record != null)
                    {
                        System.out.println(x++ + ") " + record);
                    } else if (reader.getIsCompleted())
                    {
                        System.out.println("Done Reading Records");
                        break;
                    } else {
                        //System.out.println("Sleeping");
                        Thread.sleep(10);
                    }
                }
            executorService.shutdown();
            tsLoadUtility.disconnect();
        } catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}
