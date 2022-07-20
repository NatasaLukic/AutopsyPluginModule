/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package org.ftn.messengerforensics;

import org.sleuthkit.autopsy.ingest.IngestModuleIngestJobSettings;

/**
 *
 * @author Natasa
 */
public class MessengerModuleIngestJobSettings implements IngestModuleIngestJobSettings{

    private static final long serialVersionUID = 1L;
    
    @Override
    public long getVersionNumber() {
        return serialVersionUID;
    }
    
}
