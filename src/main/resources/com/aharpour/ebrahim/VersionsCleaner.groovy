/*
 * Copyright 2013 Hippo B.V. (http://www.onehippo.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aharpour.ebrahim

import javax.jcr.ItemNotFoundException
import javax.jcr.Node
import javax.jcr.NodeIterator
import javax.jcr.RepositoryException
import javax.jcr.Session
import javax.jcr.version.Version
import javax.jcr.version.VersionHistory
import javax.jcr.version.VersionIterator
import javax.jcr.version.VersionManager

import org.hippoecm.repository.api.HippoNodeType
import org.onehippo.repository.update.BaseNodeUpdateVisitor
import org.slf4j.Logger

class VersionsCleaner extends BaseNodeUpdateVisitor {
	
	public static final String RETAIN_COUNT_PROPERTY = "hippoversionsdeleterunner.retaincount";
	public final int retainCount = 2;
	
	public Logger getLogger() {
		return log;
	}
	
	public void initialize(Session session) throws RepositoryException {
		super.initialize(session);
		String retainCountStr = null;
		getLogger().info("Running VersionsDeleteRunner with retain count {}", this.retainCount);
	}

	@Override
	public boolean doUpdate(Node node) throws RepositoryException {
		boolean hasNodeChanged = false;
		try {
			if (node.isNodeType(HippoNodeType.NT_HARDHANDLE)) {
				getLogger().info("Removing the history of node {}", node.getPath());
				removeHistory(node, node.getSession());
				hasNodeChanged = true;
			}
		} catch (RepositoryException e) {
			getLogger().error("An exception occurred while visiting a repository node.", e);
		} catch (Exception e) {
			getLogger().error("removeHistory threw an error", e);
		}
		return hasNodeChanged;
	}

	@Override
	public boolean undoUpdate(Node node) throws RepositoryException, UnsupportedOperationException {
		throw new UnsupportedOperationException('Updater does not implement undoUpdate method');
	}
	
	private void removeHistory(final Node handleNode, final Session session) throws Exception {
		int myRetainCount = this.retainCount;
		int removedCount = 0;

		final VersionManager versionManager = session.getWorkspace().getVersionManager();
		VersionHistory handleVersionHistory = versionManager.getVersionHistory(handleNode.getPath());
		final Version baseVersion = versionManager.getBaseVersion(handleNode.getPath());
		final Version rootVersion = handleVersionHistory.getRootVersion();
		List<Version> allHandleVersions = getHandleVersions(handleVersionHistory,
				Collections.unmodifiableList(Arrays.asList(rootVersion.getName(), baseVersion.getName())));

		List<String> preservedVariantVersionReferences = new ArrayList<String>();
		for (Version handleVersion : allHandleVersions) {
			//Skip (preserve) the latest #retainCount versions. For those, keep temporarily their referenced variant versions
			if (--myRetainCount >= 0) {
				preservedVariantVersionReferences.addAll(getAllVariantVersionReferences(handleVersion));
				continue;
			}
			removeVariantHistory(handleVersion, preservedVariantVersionReferences);
			handleVersionHistory.removeVersion(handleVersion.getName());
			removedCount++;
		}

		if (removedCount > 0) {
			getLogger().info("Removed {} versions", removedCount);
		}
		session.save();
	}


	private List<Version> getHandleVersions(final VersionHistory handleHistory, final List<String> exclude) throws RepositoryException {
		final List<Version> allHandleVersions = new LinkedList<Version>();
		for (VersionIterator iter = handleHistory.getAllVersions(); iter.hasNext(); ) {
			final Version handleVersion = iter.nextVersion();
			if (!exclude.contains(handleVersion.getName())) {
				allHandleVersions.add(handleVersion);
			}
		}
		Collections.reverse(allHandleVersions);
		return allHandleVersions;
	}


	private void removeVariantHistory(Version handleVersion, List<String> preservedVariantVersionReferences) throws RepositoryException {
		List<String> allVariantVersionReferences = getAllVariantVersionReferences(handleVersion);
		for (String variantVersionReference : allVariantVersionReferences) {

			//Do not delete a variant if it is used in a later (and preserved) handle version
			if (preservedVariantVersionReferences.contains(variantVersionReference)) {
				continue;
			}

			try {
				VersionHistory variantHistory = (VersionHistory) handleVersion.getSession().getNodeByIdentifier(variantVersionReference);
				for (VersionIterator childIter = variantHistory.getAllVersions(); childIter.hasNext(); ) {
					String variantVersionName = childIter.nextVersion().getName();
					if (!variantVersionName.equals("jcr:rootVersion")) {
						variantHistory.removeVersion(variantVersionName);
					}
				}
			} catch (ItemNotFoundException e) {
				//no action, item was deleted while cleaning up another handle's history
			}
		}

	}

	private List<String> getAllVariantVersionReferences(Version handleVersion) throws RepositoryException {
		List<String> allVariantVersionReferences = new ArrayList<String>();
		for (NodeIterator children = handleVersion.getNode("jcr:frozenNode").getNodes(); children.hasNext(); ) {
			Node child = children.nextNode();
			if (child.isNodeType("nt:versionedChild")) {
				allVariantVersionReferences.add(child.getProperty("jcr:childVersionHistory").getString());
			}
		}
		return allVariantVersionReferences;
	}
	
	
	
}
