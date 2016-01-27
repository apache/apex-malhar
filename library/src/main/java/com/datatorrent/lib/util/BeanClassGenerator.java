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

    addToStringMethod(classNode, allFields);
    addHashCodeMethod(classNode, allFields);
    addEqualsMethod(classNode, allFields);

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

  /**
   * Adds a toString method to underlying class. Uses StringBuilder to generate the final string.
   *
   * @param classNode
   * @param allFields
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  private static void addToStringMethod(ClassNode classNode, JSONArray allFields) throws JSONException
  {
    MethodNode toStringNode = new MethodNode(Opcodes.ACC_PUBLIC, "toString", "()Ljava/lang/String;", null, null);
    toStringNode.visitAnnotation("Ljava/lang/Override;", true);

    toStringNode.instructions.add(new TypeInsnNode(Opcodes.NEW, "java/lang/StringBuilder"));
    toStringNode.instructions.add(new InsnNode(Opcodes.DUP));
    toStringNode.instructions.add(new LdcInsnNode(classNode.name + "{"));
    toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKESPECIAL, "java/lang/StringBuilder", "<init>", "(Ljava/lang/String;)V", false));
    toStringNode.instructions.add(new VarInsnNode(Opcodes.ASTORE, 1));
    toStringNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));

    for (int i = 0; i < allFields.length(); i++) {
      JSONObject fieldObj = allFields.getJSONObject(i);
      String fieldName = fieldObj.getString(JSON_KEY_NAME);
      String fieldType = fieldObj.getString(JSON_KEY_TYPE);
      String fieldJavaType = getJavaType(fieldType);

      if (i != 0) {
        toStringNode.instructions.add(new LdcInsnNode(", "));
        toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;", false));
      }

      toStringNode.instructions.add(new LdcInsnNode(fieldName + "="));
      toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;", false));
      toStringNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
      toStringNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

      // There is no StringBuilder.append method for short and byte. It takes it as int.
      if (fieldJavaType.equals("S") || fieldJavaType.equals("B")) {
        fieldJavaType = "I";
      }
      toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(" + fieldJavaType + ")Ljava/lang/StringBuilder;", false));
    }

    toStringNode.instructions.add(new LdcInsnNode("}"));
    toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "append", "(Ljava/lang/String;)Ljava/lang/StringBuilder;", false));

    toStringNode.instructions.add(new InsnNode(Opcodes.POP));
    toStringNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    toStringNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, "java/lang/StringBuilder", "toString", "()Ljava/lang/String;", false));
    toStringNode.instructions.add(new InsnNode(Opcodes.ARETURN));

    classNode.methods.add(toStringNode);
  }

  /**
   * This will add a hashCode method for class being generated. <br>
   * Algorithm is as follows: <br>
   * <i><p>
   * int hashCode = 7;
   * for (field: all fields) {
   *   hashCode = 23 * hashCode + field.hashCode()
   * }
   * </p></i>
   * <br>
   * <b> For primitive field, hashcode implemenented is similar to the one present in its wrapper class. </b>
   *
   * @param classNode
   * @param allFields
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  private static void addHashCodeMethod(ClassNode classNode, JSONArray allFields) throws JSONException
  {
    MethodNode hashCodeNode = new MethodNode(Opcodes.ACC_PUBLIC, "hashCode", "()I", null, null);
    hashCodeNode.visitAnnotation("Ljava/lang/Override;", true);

    hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 7));
    hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ISTORE, 1));

    for (int i = 0; i < allFields.length(); i++) {
      JSONObject fieldObj = allFields.getJSONObject(i);
      String fieldName = fieldObj.getString(JSON_KEY_NAME);
      String fieldType = fieldObj.getString(JSON_KEY_TYPE);
      String fieldJavaType = getJavaType(fieldType);

      hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 23));
      hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ILOAD, 1));
      hashCodeNode.instructions.add(new InsnNode(Opcodes.IMUL));
      hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
      hashCodeNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

      switch (fieldType) {
        case "boolean":
          LabelNode falseNode = new LabelNode();
          LabelNode trueNode = new LabelNode();
          hashCodeNode.instructions.add(new JumpInsnNode(Opcodes.IFEQ, falseNode));
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.SIPUSH, 1231));
          hashCodeNode.instructions.add(new JumpInsnNode(Opcodes.GOTO, trueNode));
          hashCodeNode.instructions.add(falseNode);
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.SIPUSH, 1237));
          hashCodeNode.instructions.add(trueNode);
          break;
        case "byte":
        case "char":
        case "short":
        case "int":
          break;
        case "float":
          hashCodeNode.instructions.add(new MethodInsnNode(Opcodes.INVOKESTATIC, "java/lang/Float", "floatToIntBits", "(F)I", false));
          break;
        case "long":
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
          hashCodeNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 32));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LUSHR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LXOR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.L2I));
          break;
        case "double":
          hashCodeNode.instructions.add(new MethodInsnNode(Opcodes.INVOKESTATIC, "java/lang/Double", "doubleToLongBits", "(D)J", false));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.DUP2));
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.LSTORE, 2));
          hashCodeNode.instructions.add(new VarInsnNode(Opcodes.LLOAD, 2));
          hashCodeNode.instructions.add(new IntInsnNode(Opcodes.BIPUSH, 32));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LUSHR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.LXOR));
          hashCodeNode.instructions.add(new InsnNode(Opcodes.L2I));
          break;
        default:
          String objectOwnerType = fieldType.replace('.', '/');
          hashCodeNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, objectOwnerType, "hashCode", "()I", false));
          break;
      }
      hashCodeNode.instructions.add(new InsnNode(Opcodes.IADD));
      hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ISTORE, 1));
    }
    hashCodeNode.instructions.add(new VarInsnNode(Opcodes.ILOAD, 1));
    hashCodeNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    classNode.methods.add(hashCodeNode);
  }

  /**
   * Adds a equals method to underlying class. <br>
   * Algorithm is as follows: <br>
   * <i><p>
   * if (this == other) return true;
   * if (other == null) return false;
   * if (other is not instanceof <this class>) return false;
   * for (field: all fields) {
   * if (other.getField() != this.field) return false;
   * }
   * return true;
   * </p></i>
   * <br>
   * @param classNode
   * @param allFields
   * @throws JSONException
   */
  @SuppressWarnings("unchecked")
  private static void addEqualsMethod(ClassNode classNode, JSONArray allFields) throws JSONException
  {
    MethodNode equalsNode = new MethodNode(Opcodes.ACC_PUBLIC, "equals", "(Ljava/lang/Object;)Z", null, null);
    equalsNode.visitAnnotation("Ljava/lang/Override;", true);

    LabelNode l0 = new LabelNode();
    LabelNode l1 = new LabelNode();
    LabelNode l2 = new LabelNode();
    LabelNode l3 = new LabelNode();
    LabelNode l4 = new LabelNode();

    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));

    // if (this == other) return true;
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new JumpInsnNode(Opcodes.IF_ACMPNE, l0));
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_1));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    equalsNode.instructions.add(l0);
    // if (other == null) return false;
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new JumpInsnNode(Opcodes.IFNULL, l1));
    // if (!(other instanceof <this class>)) return false;
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new TypeInsnNode(Opcodes.INSTANCEOF, classNode.name));
    equalsNode.instructions.add(new JumpInsnNode(Opcodes.IFNE, l2));

    equalsNode.instructions.add(l1);
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_0));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    equalsNode.instructions.add(l2);
    // Check if it other object can cast to <this class>
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 1));
    equalsNode.instructions.add(new TypeInsnNode(Opcodes.CHECKCAST, classNode.name));
    equalsNode.instructions.add(new VarInsnNode(Opcodes.ASTORE, 2));

    for (int i = 0; i < allFields.length(); i++) {
      boolean isLast = ((i + 1) == allFields.length());
      JSONObject fieldObj = allFields.getJSONObject(i);
      String fieldName = fieldObj.getString(JSON_KEY_NAME);
      String fieldType = fieldObj.getString(JSON_KEY_TYPE);
      String fieldJavaType = getJavaType(fieldType);

      String getterMethodName = "get" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);
      equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 2));
      equalsNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, classNode.name, getterMethodName, "()" + fieldJavaType, false));
      equalsNode.instructions.add(new VarInsnNode(Opcodes.ALOAD, 0));
      equalsNode.instructions.add(new FieldInsnNode(Opcodes.GETFIELD, classNode.name, fieldName, fieldJavaType));

      switch (fieldType) {
        case "boolean":
        case "byte":
        case "char":
        case "short":
        case "int":
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IF_ICMPEQ : Opcodes.IF_ICMPNE, isLast ? l4 : l3));
          break;
        case "long":
          equalsNode.instructions.add(new InsnNode(Opcodes.LCMP));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFEQ : Opcodes.IFNE, isLast ? l4 : l3));
          break;
        case "float":
          equalsNode.instructions.add(new InsnNode(Opcodes.FCMPL));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFEQ : Opcodes.IFNE, isLast ? l4 : l3));
          break;
        case "double":
          equalsNode.instructions.add(new InsnNode(Opcodes.DCMPL));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFEQ : Opcodes.IFNE, isLast ? l4 : l3));
          break;
        default:
          String objectOwnerType = fieldType.replace('.', '/');
          equalsNode.instructions.add(new MethodInsnNode(Opcodes.INVOKEVIRTUAL, objectOwnerType, "equals", "(Ljava/lang/Object;)Z", false));
          equalsNode.instructions.add(new JumpInsnNode(isLast ? Opcodes.IFNE : Opcodes.IFEQ, isLast ? l4 : l3));
          break;
      }
    }

    equalsNode.instructions.add(l3);
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_0));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    equalsNode.instructions.add(l4);
    equalsNode.instructions.add(new InsnNode(Opcodes.ICONST_1));
    equalsNode.instructions.add(new InsnNode(Opcodes.IRETURN));

    classNode.methods.add(equalsNode);
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
