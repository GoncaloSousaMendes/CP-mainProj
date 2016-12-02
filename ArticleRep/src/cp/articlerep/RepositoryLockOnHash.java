package cp.articlerep;

import java.util.HashSet;

import cp.articlerep.ds.Iterator;
import cp.articlerep.ds.LinkedList;
import cp.articlerep.ds.List;
import cp.articlerep.ds.Map;
import cp.articlerep.ds.HashTable;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.Lock;

public class RepositoryLockOnHash {

	private Map<String, List<Article>> byAuthor;
	private Map<String, List<Article>> byKeyword;
	private Map<Integer, Article> byArticleId;
	private Map<Integer, ReentrantReadWriteLock> locksForId;
	private Map<String, ReentrantReadWriteLock> locksForAuthor;
	private Map<String, ReentrantReadWriteLock> locksForKeyword;

	public RepositoryLockOnHash(int nkeys) {
		this.byAuthor = new HashTable<String, List<Article>>(40000);
		this.byKeyword = new HashTable<String, List<Article>>(40000);
		this.byArticleId = new HashTable<Integer, Article>(40000);
		this.locksForId = new HashTable<Integer, ReentrantReadWriteLock>(40000);
		this.locksForAuthor = new HashTable<String, ReentrantReadWriteLock>(40000);
		this.locksForKeyword = new HashTable<String, ReentrantReadWriteLock>(40000);
		//this.initLocks();
	}


	public boolean insertArticle(Article a) {
		ReentrantReadWriteLock l = locksForId.get(a.getId());
		if ( l == null){
			l = new ReentrantReadWriteLock();
			locksForId.put(a.getId(), l);
		}
		
		l.writeLock().lock();
		if (byArticleId.contains(a.getId())){
			locksForId.get(a.getId()).writeLock().unlock();
			return false;
		}
			
		byArticleId.put(a.getId(), a);

		Iterator<String> authors = a.getAuthors().iterator();
		while (authors.hasNext()) {
			String name = authors.next();

			ReentrantReadWriteLock la = locksForAuthor.get(name);
			if(la == null){
				la = new ReentrantReadWriteLock();
				locksForAuthor.put(name, la);
			}
			la.writeLock().lock();
			List<Article> ll = byAuthor.get(name);
			if (ll == null) {
				ll = new LinkedList<Article>();
				byAuthor.put(name, ll);
			}
			ll.add(a);
			la.writeLock().unlock();
		}

		Iterator<String> keywords = a.getKeywords().iterator();
		while (keywords.hasNext()) {
			String keyword = keywords.next();

			ReentrantReadWriteLock lk = locksForKeyword.get(keyword);
			if(lk == null){
				lk = new ReentrantReadWriteLock();
				locksForKeyword.put(keyword, lk);
			}
			lk.writeLock().lock();
			List<Article> ll = byKeyword.get(keyword);
			if (ll == null) {
				ll = new LinkedList<Article>();
				byKeyword.put(keyword, ll);
			} 
			ll.add(a);
			lk.writeLock().unlock();
		}
		
		l.writeLock().unlock();
		return true;
	}

	public  void removeArticle(int id) {
		
		ReentrantReadWriteLock l = locksForId.get(id);
		if ( l == null){
			l = new ReentrantReadWriteLock();
			locksForId.put(id, l);
		}
		
		l.writeLock().lock();

		Article a = byArticleId.get(id);

		if (a == null){
			locksForId.get(id).writeLock().unlock();
			return;
		}
			
		
		
		Iterator<String> keywords = a.getKeywords().iterator();
		while (keywords.hasNext()) {
			String keyword = keywords.next();

			ReentrantReadWriteLock lk = locksForKeyword.get(keyword);

			lk.writeLock().lock();
			List<Article> ll = byKeyword.get(keyword);
			if (ll != null) {
				int pos = 0;
				Iterator<Article> it = ll.iterator();
				while (it.hasNext()) {
					Article toRem = it.next();
					if (toRem == a) {
						break;
					}
					pos++;
				}
				ll.remove(pos);
				it = ll.iterator();
				if (!it.hasNext()) { // checks if the list is empty
					byKeyword.remove(keyword);
				}
			}
			lk.writeLock().unlock();
		}

		Iterator<String> authors = a.getAuthors().iterator();
		while (authors.hasNext()) {
			String name = authors.next();

			ReentrantReadWriteLock la = locksForAuthor.get(name);

			la.writeLock().lock();
			List<Article> ll = byAuthor.get(name);
			if (ll != null) {
				int pos = 0;
				Iterator<Article> it = ll.iterator();
				while (it.hasNext()) {
					Article toRem = it.next();
					if (toRem == a) {
						break;
					}
					pos++;
				}
				ll.remove(pos);
				it = ll.iterator(); 
				if (!it.hasNext()) { // checks if the list is empty
					byAuthor.remove(name);
				}
			}
			la.writeLock().unlock();
		}

		byArticleId.remove(id);
		l.writeLock().unlock();
	}

	public  List<Article> findArticleByAuthor(List<String> authors) {

		List<Article> res = new LinkedList<Article>();

		Iterator<String> it = authors.iterator();
		while (it.hasNext()) {
			String name = it.next();

			ReentrantReadWriteLock la = locksForAuthor.get(name);
			if(la == null){
				la = new ReentrantReadWriteLock();
				locksForAuthor.put(name, la);
			}

			la.readLock().lock();
			List<Article> as = byAuthor.get(name);
			if (as != null) {
				Iterator<Article> ait = as.iterator();
				while (ait.hasNext()) {
					Article a = ait.next();
					res.add(a);
				}
			}
			la.readLock().unlock();
		}

		return res;
	}

	public  List<Article> findArticleByKeyword(List<String> keywords) {
		List<Article> res = new LinkedList<Article>();

		Iterator<String> it = keywords.iterator();
		while (it.hasNext()) {
			String keyword = it.next();

			ReentrantReadWriteLock lk = locksForKeyword.get(keyword);
			if(lk == null){
				lk = new ReentrantReadWriteLock();
				locksForKeyword.put(keyword, lk);
			}
			lk.readLock().lock();
			List<Article> as = byKeyword.get(keyword);
			if (as != null) {
				Iterator<Article> ait = as.iterator();
				while (ait.hasNext()) {
					Article a = ait.next();
					res.add(a);
				}
			}
			lk.readLock().unlock();
		}
		return res;
	}


	/**
	 * This method is supposed to be executed with no concurrent thread
	 * accessing the repository.
	 * 
	 */
	public boolean validate() {
		//return checkById();
		return checkById() && checkByAuthor() && checkByKeyword();
	}

	private boolean checkById(){
		HashSet<Integer> articleIds = new HashSet<Integer>();
		int articleCount = 0;
		//verificar se os autores e keywords existentes na hash existem nas respestivas hash's
		Iterator<Article> aIt = byArticleId.values();
		while(aIt.hasNext()) {
			Article a = aIt.next();

			//se houver repetidos, o count do hash não aumenta,
			// mas o articleCount aumenta!
			articleIds.add(a.getId());
			articleCount++;

			// check the authors consistency
			Iterator<String> authIt = a.getAuthors().iterator();
			while(authIt.hasNext()) {
				String name = authIt.next();
				if (!searchAuthorArticle(a, name)) {
					System.out.println("Check id map");
					System.out.println(name + " is missing in by author");
					return false;
				}
			}

			// check the keywords consistency
			Iterator<String> keyIt = a.getKeywords().iterator();
			while(keyIt.hasNext()) {
				String keyword = keyIt.next();
				if (!searchKeywordArticle(a, keyword)) {
					System.out.println("Check id map");
					System.out.println(keyword + " is missing in by keyword");
					return false;
				}
			}
		}

		return articleCount == articleIds.size();
	}

	private boolean searchAuthorArticle(Article a, String author) {
		List<Article> ll = byAuthor.get(author);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				if (it.next() == a) {
					return true;
				}
			}
		}
		return false;
	}

	private boolean searchKeywordArticle(Article a, String keyword) {
		List<Article> ll = byKeyword.get(keyword);
		if (ll != null) {
			Iterator<Article> it = ll.iterator();
			while (it.hasNext()) {
				if (it.next() == a) {
					return true;
				}
			}
		}
		return false;
	}

	//verificar se os artigos contidos no byAuthor existem no byID
	private boolean checkByAuthor(){

		//verificar se os autores e keywords existentes na hash existem nas respestivas hash's
		Iterator<List<Article>> aIt = byAuthor.values();
		while(aIt.hasNext()) {
			List<Article> a = aIt.next();

			// check the authors consistency
			Iterator<Article> authIt = a.iterator();
			while(authIt.hasNext()) {

				Article article = authIt.next();
				int id = article.getId();

				if (!byArticleId.contains(id)) {
					System.out.println("Check authors map");
					System.out.println(id + " is missing in by id");
					return false;
				}

				List<String> keywords = article.getKeywords();
				Iterator<String> keyIt = keywords.iterator();

				while(keyIt.hasNext()) {
					String keyword = keyIt.next();
					if (!byKeyword.contains(keyword)) {
						System.out.println("Check authors map");
						System.out.println(keyword + " is missing in by keyword");
						return false;
					}
				}
			}


		}
		return true;
	}

	private boolean checkByKeyword(){

		//verificar se os autores e keywords existentes na hash existem nas respestivas hash's
		Iterator<List<Article>> kIt = byKeyword.values();
		while(kIt.hasNext()) {
			List<Article> k = kIt.next();

			// check the authors consistency
			Iterator<Article> artIt = k.iterator();
			while(artIt.hasNext()) {

				Article article = artIt.next();
				int id = article.getId();

				if (!byArticleId.contains(id)) {
					System.out.println("Check keywords map");
					System.out.println(id + " is missing in by id");
					return false;
				}

				List<String> keywords = article.getAuthors();
				Iterator<String> authIt = keywords.iterator();

				while(authIt.hasNext()) {
					String author = authIt.next();
					if (!byAuthor.contains(author)) {
						System.out.println("Check keywords map");
						System.out.println(author + " is missing in by author");
						return false;
					}
				}
			}


		}
		return true;
	}


}
