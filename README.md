gwt-appengine
=============

A simple wrapper around AppEngine's Memcache and DataStore APIs.

Dependencies
============

* [Google Guava](http://code.google.com/p/guava-libraries/) 13.0 or above
* [Cofoja](https://code.google.com/p/cofoja/) 1.0-r139
* [Google AppEngine Sdk](https://developers.google.com/appengine/downloads) 1.7.4 or above
* [gwt-primitives](https://github.com/csavelief/gwt-primitives) 1.0 or above

What is inside ?
================

Shared :
* DataStore requests builder with no dependencies to AppEngine :
    * [fr.mncc.gwttoolbox.appengine.shared.SQuery2](https://github.com/csavelief/gwt-appengine/blob/master/src/fr/mncc/gwttoolbox/appengine/shared/SQuery2.java)

Server :
* A query converter which transforms a SQuery2 object into an AppEngine's query :
    * [fr.mncc.gwttoolbox.appengine.server.QueryConverter2](https://github.com/csavelief/gwt-appengine/blob/master/src/fr/mncc/gwttoolbox/appengine/server/QueryConverter2.java)
* A wrapper for the DataStore API (async + sync) :
    * [fr.mncc.gwttoolbox.appengine.server.DataStore2](https://github.com/csavelief/gwt-appengine/blob/master/src/fr/mncc/gwttoolbox/appengine/server/DataStore2.java)
* A wrapper for the Memcache API (async + sync) :
    * [fr.mncc.gwttoolbox.appengine.server.Memcache2](https://github.com/csavelief/gwt-appengine/blob/master/src/fr/mncc/gwttoolbox/appengine/server/Memcache2.java)

How to get started ?
====================

Download gwt-appengine.jar (built against the latest tag) and add it to your Java/GWT project classpath.

Example
=======

```java
public class ContactDTO extends Entity {

    public ContactDTO() { super("ContactDTO"); }
    public String getName() { return getAsString("name"); }
    public void setName(String name) { put("name", name); }
    public int getAge() { return getAsInt("age"); }
    public void setAge(int age) { put("age", age); }
    public String getProfilePictureAsImageBase64() { return getAsString("imageBase64"); }
    public void setProfilePictureAsImageBase64(String imageBase64) { put("imageBase64", imageBase64); }
}
```

```java
public List<ContactDTO> getContactsWhoseNameStartsBy(String prefix, int offset, int limit) {

    String prefixLowerCase = prefix == null || prefix.isEmpty() ? "" : prefix.substring(0, 1).toLowerCase() + prefix.substring(1);
    String prefixUpperCase = prefix == null || prefix.isEmpty() ? "" : prefix.substring(0, 1).toUpperCase() + prefix.substring(1);

    // Build query
    SQuery2.SClause2 clause = SQuery2.or(
        SQuery2.and(SQuery2.greaterThanOrEqual("name", prefixLowerCase), SQuery2.lessThan("name", prefixLowerCase + "\ufffd")),
        SQuery2.and(SQuery2.greaterThanOrEqual("name", prefixUpperCase), SQuery2.lessThan("name", prefixUpperCase + "\ufffd"))
    );

    SQuery2 query = new SQuery2("ContactDTO");
    query.addClause(clause);

    // Execute query
    List<ContactDTO> contacts = new ArrayList<ContactDTO>();
    for (fr.mncc.gwttoolbox.primitives.client.shared.Entity entity : DataStore2.list(squery, offset, limit))
        contacts.add(new ContactDTO(entity));

    return contacts;
}
```

License : MIT
=============

Copyright (c) 2011 [MNCC](http://www.mncc.fr/)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
