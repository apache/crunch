/**
 * Copyright (c) 2011, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */
package com.cloudera.crunch.types.avro;

import java.util.List;

import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.Path;

import com.cloudera.crunch.MapFn;
import com.cloudera.crunch.SourceTarget;
import com.cloudera.crunch.fn.IdentityFn;
import com.cloudera.crunch.io.avro.AvroFileSourceTarget;
import com.cloudera.crunch.types.Converter;
import com.cloudera.crunch.types.PType;
import com.cloudera.crunch.types.PTypeFamily;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * The implementation of the PType interface for Avro-based serialization.
 * 
 */
public class AvroType<T> implements PType<T> {

	private static final Converter AVRO_CONVERTER = new AvroKeyConverter();

	private final Class<T> typeClass;
	private final Schema schema;
	private final MapFn baseInputMapFn;
	private final MapFn baseOutputMapFn;
	private final List<PType> subTypes;

	public AvroType(Class<T> typeClass, Schema schema, PType... ptypes) {
		this(typeClass, schema, IdentityFn.getInstance(), IdentityFn.getInstance(),
				ptypes);
	}

	public AvroType(Class<T> typeClass, Schema schema, MapFn inputMapFn,
			MapFn outputMapFn, PType... ptypes) {
		this.typeClass = typeClass;
		this.schema = Preconditions.checkNotNull(schema);
		this.baseInputMapFn = inputMapFn;
		this.baseOutputMapFn = outputMapFn;
		this.subTypes = ImmutableList.<PType> builder().add(ptypes).build();
	}

	@Override
	public Class<T> getTypeClass() {
		return typeClass;
	}

	@Override
	public PTypeFamily getFamily() {
		return AvroTypeFamily.getInstance();
	}

	@Override
	public List<PType> getSubTypes() {
		return subTypes;
	}

	public Schema getSchema() {
		return schema;
	}

	/**
	 * Determine if the wrapped type is a specific or generic avro type.
	 * 
	 * @return true if the wrapped type is a specific data type
	 */
	public boolean isSpecific() {
	  if (SpecificRecord.class.isAssignableFrom(typeClass)) {
	    return true;
	  }
	  for (PType ptype : subTypes) {
	    if (SpecificRecord.class.isAssignableFrom(ptype.getTypeClass())) {
	      return true;
	    }
	  }
	  return false;
	}

	public MapFn<Object, T> getInputMapFn() {
		return baseInputMapFn;
	}

	public MapFn<T, Object> getOutputMapFn() {
		return baseOutputMapFn;
	}

	@Override
	public Converter getConverter() {
		return AVRO_CONVERTER;
	}

	@Override
	public SourceTarget<T> getDefaultFileSource(Path path) {
		return new AvroFileSourceTarget<T>(path, this);
	}

	@Override
	public boolean equals(Object other) {
		if (other == null || !(other instanceof AvroType)) {
			return false;
		}
		AvroType at = (AvroType) other;
		return (typeClass.equals(at.typeClass) && subTypes.equals(at.subTypes));

	}

	@Override
	public int hashCode() {
		HashCodeBuilder hcb = new HashCodeBuilder();
		hcb.append(typeClass).append(subTypes);
		return hcb.toHashCode();
	}

}
