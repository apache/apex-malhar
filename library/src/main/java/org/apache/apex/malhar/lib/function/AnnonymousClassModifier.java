/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.lib.function;

import org.apache.hadoop.classification.InterfaceStability;

import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.AnnotationVisitor;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Attribute;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.ClassVisitor;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.FieldVisitor;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.MethodVisitor;
import com.esotericsoftware.reflectasm.shaded.org.objectweb.asm.Opcodes;

/**
 * Because annonymous class serialization is not supported by default in most serialization library
 * This class is used to modify the bytecode of annonymous at runtime.
 * The limit for this is the annonymous class that is being modified must by stateless
 *
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class AnnonymousClassModifier extends ClassVisitor
{
  private String className;

  private boolean hasDefaultConstructor = false;

  public AnnonymousClassModifier(int i)
  {
    super(i);
  }

  public AnnonymousClassModifier(int i, ClassVisitor classVisitor)
  {
    super(i, classVisitor);
  }

  @Override
  public void visit(int i, int i1, String s, String s1, String s2, String[] strings)
  {
    className = s;
    super.visit(i, 33, s, s1, s2, strings);
  }

  @Override
  public void visitSource(String s, String s1)
  {
    super.visitSource(s, s1);
  }

  @Override
  public void visitOuterClass(String s, String s1, String s2)
  {
    // skip outer class, make it top level. For now only one level annonymous class
    return;
  }

  @Override
  public AnnotationVisitor visitAnnotation(String s, boolean b)
  {
    return super.visitAnnotation(s, b);
  }


  @Override
  public void visitAttribute(Attribute attribute)
  {
    super.visitAttribute(attribute);
  }

  @Override
  public void visitInnerClass(String s, String s1, String s2, int i)
  {
    if (s.equals(className)) {
      return;
    }
    super.visitInnerClass(s, s1, s2, i);
  }

  @Override
  public FieldVisitor visitField(int i, String s, String s1, String s2, Object o)
  {
    return super.visitField(i, s, s1, s2, o);
  }

  @Override
  public MethodVisitor visitMethod(int i, String s, String s1, String s2, String[] strings)
  {
    //make the constructor public
    int j = s.equals("<init>") ? i | Opcodes.ACC_PUBLIC : i;
    if (s1.contains("()V")) {
      hasDefaultConstructor = true;
    }

    return super.visitMethod(i, s, s1, s2, strings);
  }

  @Override
  public void visitEnd()
  {

    // If there is no default constructor, create one
    if (!hasDefaultConstructor) {
      MethodVisitor mv = super.visitMethod(Opcodes.ACC_PUBLIC, "<init>", "()V", null, null);
      mv.visitVarInsn(Opcodes.ALOAD, 0);
      mv.visitMethodInsn(Opcodes.INVOKESPECIAL,
          "java/lang/Object",
          "<init>",
          "()V");
      mv.visitInsn(Opcodes.RETURN);
      mv.visitMaxs(5, 1);
      mv.visitEnd();
    }

    super.visitEnd();
  }
}
