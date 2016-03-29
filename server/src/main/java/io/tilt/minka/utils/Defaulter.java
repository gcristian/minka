/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.utils;

import java.beans.PropertyEditor;
import java.beans.PropertyEditorManager;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javassist.Modifier;

/**
 * Configures an instance object's fields with their default static values if:
 * no key present on passed Properties, no value set to JVM as property, in that order.
 * Searches instance's class static fields as "GOOD_BYE_CRUEL_WORLD_DEFAULT" to key1 "goodByeCruelWorld"
 * in the given Properties, if no present: uses static field value to set instance object's field as key1;
 * 
 * @author Cristian Gonzalez
 * @since Dec 7, 2015
 *
 */
public class Defaulter {

    private final static Logger logger = LoggerFactory.getLogger(Defaulter.class);
    
    private final static String DELIMS = "_";
    private final static String SUFFIX = "DEFAULT";
    
    /**
     * @param   props the properties instance to look up keys for 
     * @param   configurable 
     *  applying object with pairs of "default" sufixed static fields in the format "some_value_default"
     *  and instance fields in the propercase format without underscores like "someValue" 
     * 
     * @return  TRUE if all defaults were applied. FALSE if some was not !
     */
    public static boolean apply(final Properties props, Object configurable) {
        Validate.notNull(props);
        Validate.notNull(configurable);
        boolean all = true;
        for (final Field staticField: getStaticDefaults(configurable.getClass())) {
            final String name = properCaseIt(staticField.getName());
            if (staticField.getName().endsWith(SUFFIX)) {
                final String nameNoDef = name.substring(0, name.length()-SUFFIX.length());
                try {
                    final Field instanceField = configurable.getClass().getDeclaredField(nameNoDef);
                    try {
                        final PropertyEditor editor = edit(props, configurable, staticField, instanceField);
                        instanceField.setAccessible(true);
                        instanceField.set(configurable, editor.getValue());
                    } catch (IllegalArgumentException | IllegalAccessException e) {
                        all = false;
                        logger.error("Defaulter: object <{}> cannot set value for field: {}", 
                                configurable.getClass().getSimpleName(), nameNoDef, e);
                    }
                } catch (NoSuchFieldException | SecurityException e) {
                    all = false;
                    logger.error("Defaulter: object <{}> has no field: {} for default static: {} (reason: {})", 
                            configurable.getClass().getSimpleName(), nameNoDef, 
                            staticField.getName(), e.getClass().getSimpleName());
                }
            }
        }
        return all;
    }

    private static PropertyEditor edit(
            final Properties props, 
            final Object configurable,
            final Field staticField, 
            final Field instanceField) throws IllegalAccessException {
        
        staticField.setAccessible(true);
        final String name = instanceField.getName();
        final String staticValue = staticField.get(configurable).toString();
        final Object propertyOrDefault = props.getProperty(name, System.getProperty(name, staticValue));
        final String objName = configurable.getClass().getSimpleName();
        final PropertyEditor editor = PropertyEditorManager.findEditor(instanceField.getType());
        final String setLog = "Defaulter: set <{}> field [{}] = '{}' from {} ";
        try {            
            editor.setAsText(propertyOrDefault.toString());
            logger.info(setLog, objName, name, editor.getValue(), 
                    propertyOrDefault!=staticValue ? " property " : staticField.getName());
        } catch (Exception e) {
            logger.error("Defaulter: object <{}> field: {} does not accept property or static "
                    + "default value: {} (reason: {})", objName, name, propertyOrDefault, e.getClass().getSimpleName());
            try { // at this moment only prop. might've been failed
                editor.setAsText(staticValue);
                logger.info(setLog, objName, name, editor.getValue(), staticField.getName());
            } catch (Exception e2) {
                final StringBuilder sb = new StringBuilder()
                        .append("Defaulter: object <").append(objName).append("> field: ").append(name)
                        .append(" does not accept static default value: ").append(propertyOrDefault)
                        .append(" (reason: ").append(e.getClass().getSimpleName()).append(")");
                throw new RuntimeException(sb.toString());
            }
            
        }
        
        return editor;
    }
    
    /**
     * Use delims as word beginner mark, remove it and proper case words
     * Take "HELLO_WORLD" and turn into "helloWorld" 
     * @param s
     * @return
     */
    private static String properCaseIt(final String s) {
        final StringBuilder sb = new StringBuilder();
        boolean capit = true;
        boolean first = true;
        for (char ch : s.toCharArray()) {
            ch = capit && !first ? Character.toUpperCase(ch): Character.toLowerCase(ch);
            if (DELIMS.indexOf((char)ch)<0) {
                sb.append(ch);
            }
            first = false;
            capit = (DELIMS.indexOf((int) ch) >= 0);
        }
        return sb.toString();
    }
    
    private static List<Field> getStaticDefaults(Class<?> clas) {
        final Field[] declaredFields = clas.getDeclaredFields();
        final List<Field> staticFields = new ArrayList<Field>();
        for (Field field : declaredFields) {
            if (Modifier.isStatic(field.getModifiers())) {
                staticFields.add(field);
            }
        }
        return staticFields;
    }
    

}
