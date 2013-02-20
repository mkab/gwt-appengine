/**
 * Copyright (c) 2013 MNCC
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * @author http://www.mncc.fr
 */
package fr.mncc.gwttoolbox.appengine.shared;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;

import com.google.common.base.Objects;
import com.google.gwt.user.client.rpc.IsSerializable;

import fr.mncc.gwttoolbox.primitives.shared.ObjectUtils;

public class SQuery2 implements IsSerializable, Serializable {

  public static class SFilterOperator2 implements IsSerializable, Serializable {
    public static final int EQUAL = 0;
    public static final int LESS_THAN = 1;
    public static final int LESS_THAN_OR_EQUAL = 2;
    public static final int GREATER_THAN = 3;
    public static final int GREATER_THAN_OR_EQUAL = 4;
    public static final int NOT_EQUAL = 5;
    public static final int IN = 6;

    protected SFilterOperator2() {

    }
  }

  public static class SSort2 implements IsSerializable, Serializable {

    private String propertyName_ = "";
    private boolean isAscending_ = true;

    protected SSort2() {

    }

    public SSort2(String propertyName, boolean isAscending) {
      propertyName_ = propertyName;
      isAscending_ = isAscending;
    }

    public String getPropertyName() {
      return propertyName_;
    }

    public boolean isAscending() {
      return isAscending_;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("propertyName_", propertyName_).add("isAscending_",
              isAscending_).omitNullValues().toString();
    }
  }

  public static class SFilter2 extends SClause2 implements IsSerializable, Serializable {

    private int operator_;
    private String propertyName_ = "";

    private String propertyValue_ = ""; // EQUAL, LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN,
                                        // GREATER_THAN_OR_EQUAL, NOT_EQUAL
    private ArrayList<Long> propertyValues_ = new ArrayList<Long>(); // IN

    protected SFilter2() {

    }

    public SFilter2(int operator, String propertyName, Object propertyValue) {
      if (operator != SFilterOperator2.IN) {
        operator_ = operator;
        propertyName_ = propertyName;
        propertyValue_ = ObjectUtils.toString(propertyValue);
      }
    }

    public SFilter2(String propertyName, ArrayList<Long> propertyValues) {
      operator_ = SFilterOperator2.IN;
      propertyName_ = propertyName;
      propertyValues_ = propertyValues;
    }

    public int getOperator() {
      return operator_;
    }

    public String getPropertyName() {
      return propertyName_;
    }

    public Object getPropertyValue() {
      return ObjectUtils.fromString(propertyValue_);
    }

    public ArrayList<Long> getPropertyValues() {
      return propertyValues_;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("operator_", operator_).add("propertyName_",
              propertyName_).add("propertyValue_", propertyValue_).add("propertyValues_",
              propertyValues_).omitNullValues().toString();
    }
  }

  public static class SClause2 implements IsSerializable, Serializable {

    private boolean isAnd_ = true;
    private SClause2 clauseLeft_ = null;
    private SClause2 clauseRight_ = null;

    protected SClause2() {

    }

    public SClause2(boolean isAnd, SClause2 clauseLeft, SClause2 clauseRight) {
      isAnd_ = isAnd;
      clauseLeft_ = clauseLeft;
      clauseRight_ = clauseRight;
    }

    public boolean isAnd() {
      return isAnd_;
    }

    public SClause2 getLeftClause() {
      return clauseLeft_;
    }

    public SClause2 getRightClause() {
      return clauseRight_;
    }

    public boolean isNode() {
      return !isLeaf();
    }

    public boolean isLeaf() {
      return clauseLeft_ == null && clauseRight_ == null;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("isAnd_", isAnd_).add("clauseLeft_", clauseLeft_)
          .add("clauseRight_", clauseRight_).omitNullValues().toString();
    }
  }

  public static class SProjection2 implements IsSerializable, Serializable {

    private String propertyName_ = "";
    private Class<?> clazz_ = null;

    protected SProjection2() {

    }

    public SProjection2(String propertyName, Class<?> clazz) {
      propertyName_ = propertyName;
      clazz_ = clazz;
    }

    public String getPropertyName() {
      return propertyName_;
    }

    public Class<?> getClazz() {
      return clazz_;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("propertyName_", propertyName_).add("clazz_", clazz_)
          .omitNullValues().toString();
    }
  }

  private String kind_;
  private String ancestorKind_;
  private long ancestorId_;

  private ArrayList<SProjection2> projections_ = new ArrayList<SProjection2>();
  private ArrayList<SSort2> sorters_ = new ArrayList<SSort2>();
  private SClause2 clause_ = null;
  private boolean isKeysOnly_ = false;

  public static SClause2 idEqual(long propertyValue) {
    return new SFilter2(SFilterOperator2.EQUAL, "__key__", propertyValue);
  }

  public static SClause2 idLessThan(long propertyValue) {
    return new SFilter2(SFilterOperator2.LESS_THAN, "__key__", propertyValue);
  }

  public static SClause2 idLessThanOrEqual(long propertyValue) {
    return new SFilter2(SFilterOperator2.LESS_THAN_OR_EQUAL, "__key__", propertyValue);
  }

  public static SClause2 idGreaterThan(long propertyValue) {
    return new SFilter2(SFilterOperator2.GREATER_THAN, "__key__", propertyValue);
  }

  public static SClause2 idGreaterThanOrEqual(long propertyValue) {
    return new SFilter2(SFilterOperator2.GREATER_THAN_OR_EQUAL, "__key__", propertyValue);
  }

  public static SClause2 idNotEqual(Object propertyValue) {
    return new SFilter2(SFilterOperator2.NOT_EQUAL, "__key__", propertyValue);
  }

  public static SClause2 idIn(ArrayList<Long> propertyValues) {
    return new SFilter2("__key__", propertyValues);
  }

  public static SClause2 equal(String propertyName, Object propertyValue) {
    return new SFilter2(SFilterOperator2.EQUAL, propertyName, propertyValue);
  }

  public static SClause2 lessThan(String propertyName, Object propertyValue) {
    return new SFilter2(SFilterOperator2.LESS_THAN, propertyName, propertyValue);
  }

  public static SClause2 lessThanOrEqual(String propertyName, Object propertyValue) {
    return new SFilter2(SFilterOperator2.LESS_THAN_OR_EQUAL, propertyName, propertyValue);
  }

  public static SClause2 greaterThan(String propertyName, Object propertyValue) {
    return new SFilter2(SFilterOperator2.GREATER_THAN, propertyName, propertyValue);
  }

  public static SClause2 greaterThanOrEqual(String propertyName, Object propertyValue) {
    return new SFilter2(SFilterOperator2.GREATER_THAN_OR_EQUAL, propertyName, propertyValue);
  }

  public static SClause2 notEqual(String propertyName, Object propertyValue) {
    return new SFilter2(SFilterOperator2.NOT_EQUAL, propertyName, propertyValue);
  }

  public static SClause2 in(String propertyName, ArrayList<Long> propertyValues) {
    return new SFilter2(propertyName, propertyValues);
  }

  public static SClause2 and(SClause2 clauseLeft, SClause2 clauseRight) {
    return new SClause2(true, clauseLeft, clauseRight);
  }

  public static SClause2 or(SClause2 clauseLeft, SClause2 clauseRight) {
    return new SClause2(false, clauseLeft, clauseRight);
  }

  protected SQuery2() {
    this("", "", 0);
  }

  public SQuery2(String kind) {
    this(kind, "", 0);
  }

  public SQuery2(String kind, String ancestorKind, long ancestorId) {
    kind_ = kind;
    ancestorKind_ = ancestorKind;
    ancestorId_ = ancestorId;
  }

  public String getKind() {
    return kind_;
  }

  public String getAncestorKind() {
    return ancestorKind_;
  }

  public long getAncestorId() {
    return ancestorId_;
  }

  public ArrayList<SProjection2> getProjections() {
    return projections_;
  }

  public ArrayList<SSort2> getSorters() {
    return sorters_;
  }

  public SClause2 getClause() {
    return clause_;
  }

  public boolean isKeysOnly() {
    return isKeysOnly_;
  }

  public SQuery2 setKeysOnly() {
    isKeysOnly_ = true;
    return this;
  }

  public SQuery2 removeKeysOnly() {
    isKeysOnly_ = false;
    return this;
  }

  public SQuery2 addStringProjection(String propertyName) {
    if (isValidProjection(propertyName))
      projections_.add(new SProjection2(propertyName, String.class));
    return this;
  }

  public SQuery2 addLongProjection(String propertyName) {
    if (isValidProjection(propertyName))
      projections_.add(new SProjection2(propertyName, Long.class));
    return this;
  }

  public SQuery2 addFloatProjection(String propertyName) {
    if (isValidProjection(propertyName))
      projections_.add(new SProjection2(propertyName, Float.class));
    return this;
  }

  public SQuery2 addBooleanProjection(String propertyName) {
    if (isValidProjection(propertyName))
      projections_.add(new SProjection2(propertyName, Boolean.class));
    return this;
  }

  public SQuery2 addDateProjection(String propertyName) {
    if (isValidProjection(propertyName))
      projections_.add(new SProjection2(propertyName, Date.class));
    return this;
  }

  public SQuery2 removeProjections() {
    projections_.clear();
    return this;
  }

  public SQuery2 addClause(SClause2 clause) {
    if (isValidClause(clause))
      clause_ = clause;
    return this;
  }

  public SQuery2 removeClause() {
    clause_ = null;
    return this;
  }

  public SQuery2 addIdAscendingSorter() {
    sorters_.add(new SSort2("__key__", true));
    return this;
  }

  public SQuery2 addIdDescendingSorter() {
    sorters_.add(new SSort2("__key__", false));
    return this;
  }

  public SQuery2 addAscendingSorter(String propertyName) {
    sorters_.add(new SSort2(propertyName, true));
    return this;
  }

  public SQuery2 addDescendingSorter(String propertyName) {
    sorters_.add(new SSort2(propertyName, false));
    return this;
  }

  public SQuery2 removeSorters() {
    sorters_.clear();
    return this;
  }

  private boolean isValidProjection(String propertyName) {
    if (propertyName == null || propertyName.isEmpty())
      return false;

    // The same property cannot be projected more than once
    if (projections_ != null) {
      for (SProjection2 projection : projections_) {
        if (projection.getPropertyName().equals(propertyName))
          return false;
      }
    }

    // Properties referenced in an equality (EQUAL) or membership (IN) filter cannot be projected
    if (clause_ != null)
      return isValidProjection(propertyName, clause_);
    return true;
  }

  private boolean isValidProjection(String propertyName, SClause2 clause) {
    if (!clause.isLeaf())
      return isValidProjection(propertyName, clause.getLeftClause())
          && isValidProjection(propertyName, clause.getRightClause());

    SFilter2 sfilter = (SFilter2) clause;
    if (sfilter.getOperator() != SFilterOperator2.IN
        && sfilter.getOperator() != SFilterOperator2.EQUAL)
      return true;
    return !sfilter.getPropertyName().equals(propertyName);
  }

  private boolean isValidClause(SClause2 clause) {
    if (clause == null)
      return false;

    // Properties referenced in an equality (EQUAL) or membership (IN) filter cannot be projected
    if (projections_ != null) {
      for (SProjection2 projection : projections_) {
        if (!isValidProjection(projection.getPropertyName(), clause))
          return false;
      }
    }

    // TODO :
    // - Inequality filters are limited to at most one property
    // - Properties used in inequality filters must be sorted first

    return clause.isLeaf() ? true : isValidClause(clause.getLeftClause())
        && isValidClause(clause.getRightClause());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("kind_", kind_).add("ancestorKind_", ancestorKind_)
        .add("ancestorId_", ancestorId_).add("projections_", projections_)
        .add("sorters_", sorters_).add("clause_", clause_).add("isKeysOnly_", isKeysOnly_)
        .omitNullValues().toString();
  }
}
