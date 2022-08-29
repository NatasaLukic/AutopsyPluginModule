/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.ftn.messengerforensics;
// The following import is required for the ServiceProvider annotation (see 
// below) used by the Autopsy ingest framework to locate ingest module 
// factories. You will need to add a dependency on the Lookup API NetBeans 
// module to your NetBeans module to use this import.
import org.openide.util.lookup.ServiceProvider;
// The following import is required to participate in Autopsy 
// internationalization and localization. Autopsy core is currently localized 
// for Japan. Please consult the NetBeans documentation for details.
import org.openide.util.NbBundle;
import org.sleuthkit.autopsy.ingest.IngestModuleFactory;
import org.sleuthkit.autopsy.ingest.DataSourceIngestModule;
import org.sleuthkit.autopsy.ingest.IngestModuleFactoryAdapter;
import org.sleuthkit.autopsy.ingest.IngestModuleIngestJobSettings;

/**
 *
 * @author Natasa
 */
@ServiceProvider(service = IngestModuleFactory.class)
public class MessengerIngestModuleFactory extends IngestModuleFactoryAdapter {
    private static final String VERSION_NUMBER = "1.0.0";
    
    static String getModuleName() {
        return NbBundle.getMessage(MessengerIngestModuleFactory.class, "MessengerIngestModuleFactory.moduleName");
    }

    @Override
    public String getModuleDisplayName() {
        return getModuleName();
    }

    @Override
    public String getModuleDescription() {
      return NbBundle.getMessage(MessengerIngestModuleFactory.class, "MessengerIngestModuleFactory.moduleDescription");
    }

    @Override
    public String getModuleVersionNumber() {
        return VERSION_NUMBER;
    }
    
    @Override
    public boolean isDataSourceIngestModuleFactory() {
        return true;
    }
 
    @Override
    public DataSourceIngestModule createDataSourceIngestModule(IngestModuleIngestJobSettings settings) {
        return new MessengerDataSourceIngestModule();
    }
}
