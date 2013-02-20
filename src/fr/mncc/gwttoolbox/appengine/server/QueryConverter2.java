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
package fr.mncc.gwttoolbox.appengine.server;

import java.util.ArrayList;
import java.util.List;

import com.google.appengine.api.datastore.Key;
import com.google.appengine.api.datastore.KeyFactory;
import com.google.appengine.api.datastore.PropertyProjection;
import com.google.appengine.api.datastore.Query;

import fr.mncc.gwttoolbox.appengine.shared.SQuery2;

public class QueryConverter2 {

  public static Query getAsAppEngineQuery(SQuery2 squery) {
    Query query =
        hasAncestor(squery) ? new Query(squery.getKind(), KeyFactory.createKey(squery
            .getAncestorKind(), squery.getAncestorId())) : new Query(squery.getKind());

    // Set keys only
    if (squery.isKeysOnly())
      query.setKeysOnly();

    // Add projections
    for (SQuery2.SProjection2 projection : squery.getProjections())
      query.addProjection(new PropertyProjection(projection.getPropertyName(), projection
          .getClazz()));

    // Add sort order
    for (SQuery2.SSort2 sorter : squery.getSorters())
      query.addSort(sorter.getPropertyName(), sorter.isAscending() ? Query.SortDirection.ASCENDING
          : Query.SortDirection.DESCENDING);

    // Add clause
    if (squery.getClause() != null)
      query.setFilter(buildClause(squery, squery.getClause()));
    return query;
  }

  private static Query.Filter buildClause(SQuery2 squery, SQuery2.SClause2 clause) {
    if (clause.isLeaf()) {
      SQuery2.SFilter2 sfilter = (SQuery2.SFilter2) clause;
      if (sfilter.getOperator() != SQuery2.SFilterOperator2.IN) {
        if (sfilter.getPropertyName().equals("__key__")
            && sfilter.getPropertyValue() instanceof Long) {
          if (hasAncestor(squery))
            return new Query.FilterPredicate(sfilter.getPropertyName(), getAsFilterOperator(sfilter
                .getOperator()), KeyFactory.createKey(KeyFactory.createKey(
                squery.getAncestorKind(), squery.getAncestorId()), squery.getKind(), (Long) sfilter
                .getPropertyValue()));
          return new Query.FilterPredicate(sfilter.getPropertyName(), getAsFilterOperator(sfilter
              .getOperator()), KeyFactory.createKey(squery.getKind(), (Long) sfilter
              .getPropertyValue()));
        }
        return new Query.FilterPredicate(sfilter.getPropertyName(), getAsFilterOperator(sfilter
            .getOperator()), sfilter.getPropertyValue());
      }

      List<Key> keys = new ArrayList<Key>();
      for (Long id : sfilter.getPropertyValues()) {
        if (hasAncestor(squery))
          keys.add(KeyFactory.createKey(KeyFactory.createKey(squery.getAncestorKind(), squery
              .getAncestorId()), squery.getKind(), id));
        else
          keys.add(KeyFactory.createKey(squery.getKind(), id));
      }
      return new Query.FilterPredicate("__key__", Query.FilterOperator.IN, keys);
    }
    if (clause.isAnd())
      return Query.CompositeFilterOperator.and(buildClause(squery, clause.getLeftClause()),
          buildClause(squery, clause.getRightClause()));
    return Query.CompositeFilterOperator.or(buildClause(squery, clause.getLeftClause()),
        buildClause(squery, clause.getRightClause()));
  }

  private static Query.FilterOperator getAsFilterOperator(int operator) {
    if (operator == SQuery2.SFilterOperator2.EQUAL)
      return Query.FilterOperator.EQUAL;
    else if (operator == SQuery2.SFilterOperator2.LESS_THAN)
      return Query.FilterOperator.LESS_THAN;
    else if (operator == SQuery2.SFilterOperator2.LESS_THAN_OR_EQUAL)
      return Query.FilterOperator.LESS_THAN_OR_EQUAL;
    else if (operator == SQuery2.SFilterOperator2.GREATER_THAN)
      return Query.FilterOperator.GREATER_THAN;
    else if (operator == SQuery2.SFilterOperator2.GREATER_THAN_OR_EQUAL)
      return Query.FilterOperator.GREATER_THAN_OR_EQUAL;
    else if (operator == SQuery2.SFilterOperator2.NOT_EQUAL)
      return Query.FilterOperator.NOT_EQUAL;
    else if (operator == SQuery2.SFilterOperator2.IN)
      return Query.FilterOperator.IN;
    return null; // This case should never happen
  }

  private static boolean hasAncestor(SQuery2 squery) {
    return squery.getAncestorKind() != null && !squery.getAncestorKind().isEmpty()
        && squery.getAncestorId() > 0;
  }
}
