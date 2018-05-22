package com.t3c.anchel.storageregistration;

import java.net.BindException;
import java.util.List;

public interface AbstractRepository<T> {

	T create(T entity) throws BindException, IllegalArgumentException;

	/**
	 * Attach the entity to the session context.
	 * 
	 * @param entity
	 *            the entity to load.
	 * @return attached entity.
	 * @throws org.linagora.linshare.core.exception.BusinessException
	 *             in case of failure.
	 * @throws java.lang.IllegalArgumentException
	 *             if entity is null or if the type of the give entity doesn't
	 *             match with the type of the element retrieved in database..
	 */
	T load(T entity) throws BindException, IllegalArgumentException;

	/**
	 * Update the provided entity.
	 * 
	 * @param entity
	 *            the entity to update.
	 * @return persisted entity.
	 * @throws org.linagora.linshare.core.exception.BusinessException
	 *             in case of failure.
	 * @throws java.lang.IllegalArgumentException
	 *             if entity is null.
	 */
	T update(T entity) throws BindException, IllegalArgumentException;

	/**
	 * Find all entities of this type.
	 * 
	 * @return result list.
	 */
	List<T> findAll();

	/**
	 * Delete the provided entity.
	 * 
	 * @param entity
	 *            the entity to delete.
	 * @throws org.linagora.linshare.core.exception.BusinessException
	 *             in case of failure.
	 * @throws java.lang.IllegalArgumentException
	 *             if entity is null.
	 */

}
