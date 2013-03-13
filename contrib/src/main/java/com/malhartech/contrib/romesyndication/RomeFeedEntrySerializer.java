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
 * Serializer for RomeFeedEntry in Kryo framework. <p><br>
 *
 * <br>
 * The serializer has methods to serialize and deserialize a RomeFeedEntry.
 * It uses Java serialization to actually convert the object into byte array before
 * writing it out using Kryo. Similarly it uses java deserialization to create the
 * object from byte array read from Kryo. It uses a simple scheme to notify the
 * deserialzer about the length of the serialized object by serializing the length
 * and including it before the serialized object. The deserializer can use this information
 * to determine how many bytes to read and use for deserialization of the object.
 * This scheme is generic enough to be used for any object other than a
 * RomeFeedEntry. <br>
 * <br>
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class RomeFeedEntrySerializer extends Serializer<RomeFeedEntry> {

    /**
     * Serialize the RomeFeedEntry and write to output.
     * @param kryo The kryo context
     * @param output The output to write the serialized contents to
     * @param t The RomeFeedEntry to serialize
     */
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

    /**
     * Deserialize data from input, create a RomeFeedEntry and return it.
     * @param kryo The Kryo context
     * @param input The input to read serialized data from
     * @param type The class of RomeFeedEntry
     * @return The deserialized RomeFeedEntry object
     */
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
