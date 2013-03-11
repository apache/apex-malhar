/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.romesyndication;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.syndication.feed.synd.SyndEntry;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 *
 * @author pramod
 */
public class RomeFeedEntrySerializer extends Serializer<RomeFeedEntry> {

    @Override
    public void write(Kryo kryo, Output output, RomeFeedEntry t) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(t.getSyndEntry());
            oos.close();
            byte[] ba = baos.toByteArray();
            output.writeInt(ba.length);
            output.write(ba);
            //System.out.println("enc len " + ba.length);
        } catch (Exception ex) {
            throw new RuntimeException(ex.getMessage());
        }
    }

    @Override
    public RomeFeedEntry read(Kryo kryo, Input input, Class<RomeFeedEntry> type) {
        RomeFeedEntry t = null;
        try {
            int length = input.readInt();
            byte[] ba = new byte[length];
            input.readBytes(ba);
            //System.out.println("des len " + ba.length);
            ByteArrayInputStream bais = new ByteArrayInputStream(ba);
            ObjectInputStream ois = new ObjectInputStream(bais);
            SyndEntry syndEntry = (SyndEntry)ois.readObject();
            t = new RomeFeedEntry(syndEntry);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex.getMessage());
        }
        return t;
    }        
    
}
