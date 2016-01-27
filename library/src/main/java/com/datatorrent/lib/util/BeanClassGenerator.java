/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.gateway.schema;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.xbean.asm5.ClassWriter;
import org.apache.xbean.asm5.Opcodes;
import org.apache.xbean.asm5.tree.*;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import com.datatorrent.stram.webapp.asm.MethodNode;

/**
 * Creates a bean class on fly.
 */
public class BeanClassGenerator
{
  private static final String JSON_KEY_NAME = "name";
  private static final String JSON_KEY_FIELDS = "fields";
  private static final String JSON_KEY_TYPE = "type";

  public static final ImmutableMap<String, Character> PRIMITIVE_TYPES;

  static {
    Map<String, Character> types = Maps.newHashMap();
    types.put("boolean", 'Z');
    types.put("char", 'C');
    types.put("byte", 'B');
    types.put("short", 'S');
    types.put("int", 'I');
    types.put("float", 'F');
    types.put("long", 'J');
    types.put("double", 'D');
    PRIMITIVE_TYPES = ImmutableMap.copyOf(types);
  }

  /**
   * Creates a class from the json and writes it to the output stream.
   *
   * @param fqcn         fully qualified class name
   * @param jsonObject   json describing the class
   * @param outputStream stream to which the class is persisted
   * @throws JSONException
   * @throws IOException
   */
  @SuppressWarnings("unchecked")
  public static void createAndWriteBeanClass(String fqcn, JSONObject jsonObject,
                                             FSDataOutputStream outputStream) throws JSONException, IOException
  {
    ClassNode classNode = new ClassNode();

    classNode.version = Opcodes.V1_6;  //generated class will only run on JRE 1.6 or above
    classNode.access = Opcodes.ACC_PUBLIC;

    classNode.name = fqcn.replace('.', '/');
    classNode.superName = "java/lang/Object";

    // add default constructor
    MethodNode constructorNode = new MethodNode(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
    constructorNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    constructorNode.instructions.add(new MethodInsnNode(Opcodes.INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false));
    constructorNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(constructorNode);

    //for each field in json add a field to this class and a getter and setter for it.
    JSONArray allFields = jsonObject.getJSONArray(JSON_KEY_FIELDS);
    for (int i = 0; i < allFields.length(); i++) {
      JSONObject fieldObj = allFields.getJSONObject(i);
      String fieldName = fieldObj.getString(JSON_KEY_NAME);
      String fieldType = fieldObj.getString(JSON_KEY_TYPE);
      String fieldJavaType = getJavaType(fieldType);

      // Add private field
      FieldNode fieldNode = new FieldNode(Opcodes.ACC_PRIVATE, fieldName, fieldJavaType, null, null);
      classNode.fields.add(fieldNode);

      String fieldNameForMethods = Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

      switch (fieldType) {
        case "boolean":
        case "byte":
        case "char":
        case "short":
        case "int":
          addIntGetterNSetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
          break;
        case "long":
          addLongGetterNSetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
          break;
        case "float":
          addFloatGetterNSetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
          break;
        case "double":
          addDoubleGetterNSetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
          break;
        default:
          addObjectGetterNSetter(classNode, fieldName, fieldNameForMethods, fieldJavaType);
          break;
      }
    }

    //Write the class
    ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_MAXS | ClassWriter.COMPUTE_FRAMES);
    classNode.accept(cw);
    cw.visitEnd();
    outputStream.write(cw.toByteArray());
    outputStream.close();
  }

  @SuppressWarnings("unchecked")
  private static void addIntGetterNSetter(ClassNode classNode, String fieldName, String fieldNameForMethods,
                                          String fieldJavaType)
  {
    // Create getter
    String getterSignature = "()" + fieldJavaType;
    MethodNode getterNode = new MethodNode(Opcodes.ACC_PUBLIC, "get" + fieldNameForMethods, getterSignature, null, null);
    getterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    getterNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
    getterNode.instructions.add(new InsnNode(Opcodes.IRETURN));
    classNode.methods.add(getterNode);

    // Create setter
    String setterSignature = '(' + fieldJavaType + ')' + 'V';
    MethodNode setterNode = new MethodNode(Opcodes.ACC_PUBLIC, "set" + fieldNameForMethods, setterSignature, null, null);
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    setterNode.instructions.add(new VarInsnNode(Opcodes.ILOAD, 1));
    setterNode.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, classNode.name, fieldName, fieldJavaType));
    setterNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(setterNode);

  }

  @SuppressWarnings("unchecked")
  private static void addLongGetterNSetter(ClassNode classNode, String fieldName, String fieldNameForMethods,
                                           String fieldJavaType)
  {
    // Create getter
    String getterSignature = "()" + fieldJavaType;
    MethodNode getterNode = new MethodNode(Opcodes.ACC_PUBLIC, "get" + fieldNameForMethods, getterSignature, null, null);
    getterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    getterNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
    getterNode.instructions.add(new InsnNode(Opcodes.LRETURN));
    classNode.methods.add(getterNode);

    // Create setter
    String setterSignature = '(' + fieldJavaType + ')' + 'V';
    MethodNode setterNode = new MethodNode(Opcodes.ACC_PUBLIC, "set" + fieldNameForMethods, setterSignature, null, null);
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    setterNode.instructions.add(new VarInsnNode(Opcodes.LLOAD, 1));
    setterNode.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, classNode.name, fieldName, fieldJavaType));
    setterNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(setterNode);
  }

  @SuppressWarnings("unchecked")
  private static void addFloatGetterNSetter(ClassNode classNode, String fieldName, String fieldNameForMethods,
                                            String fieldJavaType)
  {
    // Create getter
    String getterSignature = "()" + fieldJavaType;
    MethodNode getterNode = new MethodNode(Opcodes.ACC_PUBLIC, "get" + fieldNameForMethods, getterSignature, null, null);
    getterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    getterNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
    getterNode.instructions.add(new InsnNode(Opcodes.FRETURN));
    classNode.methods.add(getterNode);

    // Create setter
    String setterSignature = '(' + fieldJavaType + ')' + 'V';
    MethodNode setterNode = new MethodNode(Opcodes.ACC_PUBLIC, "set" + fieldNameForMethods, setterSignature, null, null);
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    setterNode.instructions.add(new VarInsnNode(Opcodes.FLOAD, 1));
    setterNode.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, classNode.name, fieldName, fieldJavaType));
    setterNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(setterNode);
  }

  @SuppressWarnings("unchecked")
  private static void addDoubleGetterNSetter(ClassNode classNode, String fieldName, String fieldNameForMethods,
                                             String fieldJavaType)
  {
    // Create getter
    String getterSignature = "()" + fieldJavaType;
    MethodNode getterNode = new MethodNode(Opcodes.ACC_PUBLIC, "get" + fieldNameForMethods, getterSignature, null, null);
    getterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    getterNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
    getterNode.instructions.add(new InsnNode(Opcodes.DRETURN));
    classNode.methods.add(getterNode);

    // Create setter
    String setterSignature = '(' + fieldJavaType + ')' + 'V';
    MethodNode setterNode = new MethodNode(Opcodes.ACC_PUBLIC, "set" + fieldNameForMethods, setterSignature, null, null);
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    setterNode.instructions.add(new VarInsnNode(Opcodes.DLOAD, 1));
    setterNode.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, classNode.name, fieldName, fieldJavaType));
    setterNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(setterNode);
  }

  @SuppressWarnings("unchecked")
  private static void addObjectGetterNSetter(ClassNode classNode, String fieldName, String fieldNameForMethods,
                                             String fieldJavaType)
  {
    // Create getter
    String getterSignature = "()" + fieldJavaType;
    MethodNode getterNode = new MethodNode(Opcodes.ACC_PUBLIC, "get" + fieldNameForMethods, getterSignature, null, null);
    getterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    getterNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
    getterNode.instructions.add(new InsnNode(Opcodes.ARETURN));
    classNode.methods.add(getterNode);

    // Create setter
    String setterSignature = '(' + fieldJavaType + ')' + 'V';
    MethodNode setterNode = new MethodNode(Opcodes.ACC_PUBLIC, "set" + fieldNameForMethods, setterSignature, null, null);
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
    setterNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    setterNode.instructions.add(new FieldInsnNode(Opcodes.PUTFIELD, classNode.name, fieldName, fieldJavaType));
    setterNode.instructions.add(new InsnNode(Opcodes.RETURN));
    classNode.methods.add(setterNode);
  }

  private static String getJavaType(String fieldType)
  {
    Character pchar = PRIMITIVE_TYPES.get(fieldType);
    if (pchar != null) {
      //it is a primitive type
      return Character.toString(pchar);
    }
    //non-primitive so find the internal name of the class.
    return 'L' + fieldType.replace('.', '/') + ';';
  }

  /**
   * Given the class name it reads and loads the class from the input stream.
   *
   * @param fqcn        fully qualified class name.
   * @param inputStream stream from which class is read.
   * @return loaded class
   * @throws IOException
   */
  public static Class<?> readBeanClass(String fqcn, FSDataInputStream inputStream) throws IOException
  {
    byte[] bytes = IOUtils.toByteArray(inputStream);
    inputStream.close();
    return new ByteArrayClassLoader().defineClass(fqcn, bytes);
  }

  private static class ByteArrayClassLoader extends ClassLoader
  {
    Class<?> defineClass(String name, byte[] ba)
    {
      return defineClass(name, ba, 0, ba.length);
    }
  }
}
