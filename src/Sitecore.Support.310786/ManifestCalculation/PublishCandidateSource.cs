namespace Sitecore.Support.Framework.Publishing.ManifestCalculation
{
  using Sitecore.Framework.Conditions;
  using Sitecore.Framework.Publishing;
  using Sitecore.Framework.Publishing.Item;
  using Sitecore.Framework.Publishing.Locators;
  using Sitecore.Framework.Publishing.ManifestCalculation;
  using Sitecore.Framework.Publishing.TemplateGraph;
  using Sitecore.Framework.Publishing.Workflow;
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Threading;
  using System.Threading.Tasks;

  public class PublishCandidateSource : IPublishCandidateSource
  {
    private class CacheablePublishable
    {
      public IPublishCandidate Node
      {
        get;
      }

      public CacheablePublishable Parent
      {
        get;
      }

      public CacheablePublishable(IPublishCandidate node, CacheablePublishable parent)
      {
        Node = node;
        Parent = parent;
      }
    }

    private static readonly DateTime MaxUtc;

    private static readonly DateTime MinUtc;

    private static readonly IVarianceIdentifierComparer IdentifierComparer;

    private readonly Dictionary<Guid, CacheablePublishable> _ancestorCache = new Dictionary<Guid, CacheablePublishable>();

    private readonly SemaphoreSlim _ancestorLock = new SemaphoreSlim(1);

    private readonly Guid[] _mediaFieldsIds;

    private readonly bool _contentAvailabilityEnabled;

    private readonly string _sourceStore;

    private readonly Dictionary<Guid, IPublishableWorkflowState> _publishableStates;

    private readonly ITemplateGraph _publishingTemplateGraph;

    private readonly ICompositeItemReadRepository _itemReadRepo;

    private readonly IItemRelationshipRepository _itemRelationshipRepo;

    private readonly NodeQueryContext _queryContext;

    public PublishCandidateSource(string sourceStore, ICompositeItemReadRepository itemReadRepo, IItemRelationshipRepository itemRelationshipRepo, ITemplateGraph publishingTemplateGraph, IWorkflowStateRepository workflowRepo, Language[] publishLanguages, Guid[] publishFields, Guid[] mediaFieldIds, bool contentAvailabilityEnabled)
    {
      Condition.Requires(sourceStore, "sourceStore").IsNotNull();
      Condition.Requires(itemReadRepo, "itemReadRepo").IsNotNull();
      Condition.Requires(itemRelationshipRepo, "itemRelationshipRepo").IsNotNull();
      Condition.Requires(publishingTemplateGraph, "publishingTemplateGraph").IsNotNull();
      Condition.Requires(workflowRepo, "workflowRepo").IsNotNull();
      Condition.Requires(publishLanguages, "publishLanguages").IsNotNull();
      Condition.Requires(publishFields, "publishFields").IsNotNull();
      Condition.Requires(mediaFieldIds, "mediaFieldIds").IsNotNull();
      _sourceStore = sourceStore;
      _mediaFieldsIds = mediaFieldIds;
      _contentAvailabilityEnabled = contentAvailabilityEnabled;
      _itemReadRepo = itemReadRepo;
      _itemRelationshipRepo = itemRelationshipRepo;
      _publishingTemplateGraph = publishingTemplateGraph;
      _queryContext = new NodeQueryContext(publishLanguages, publishFields);
      _publishableStates = workflowRepo.GetPublishableStates(PublishingConstants.WorkflowFields.Final, PublishingConstants.WorkflowFields.PreviewPublishTarget).Result.ToDictionary((IPublishableWorkflowState s) => s.StateId, (IPublishableWorkflowState s) => s);
    }

    public async Task<IPublishCandidate> GetNode(Guid id)
    {
      IItemNode itemNode = await _itemReadRepo.GetItemNode(id, _queryContext).ConfigureAwait(false);
      if (itemNode == null)
      {
        return null;
      }
      return BuildPublishable(itemNode);
    }

    public async Task<IEnumerable<IPublishCandidate>> GetNodes(IReadOnlyCollection<Guid> ids)
    {
      return (await _itemReadRepo.GetItemNodes(ids, _queryContext).ConfigureAwait(false)).Select(delegate (IItemNode n)
      {
        if (n == null)
        {
          return null;
        }
        return BuildPublishable(n);
      });
    }

    public async Task<IEnumerable<IPublishCandidate>> GetAncestors(IPublishCandidate node)
    {
      await _ancestorLock.WaitAsync();
      try
      {
        if (!node.ParentId.HasValue)
        {
          return Enumerable.Empty<IPublishCandidate>();
        }
        CacheablePublishable fromCache = GetFromCache(node.ParentId.Value);
        if (fromCache != null)
        {
          return YieldAncestorChain(fromCache);
        }
        IEnumerable<Guid> obj = await _itemReadRepo.GetItemNodeAncestors(node.Id).ConfigureAwait(false);
        List<Guid> list = new List<Guid>();
        CacheablePublishable rootInCache = null;
        foreach (Guid item in obj)
        {
          rootInCache = GetFromCache(item);
          if (rootInCache != null)
          {
            break;
          }
          list.Add(item);
        }
        if (list.Any())
        {
          list.Reverse();
          foreach (IItemNode item2 in await _itemReadRepo.GetItemNodes((IReadOnlyCollection<Guid>)list.ToArray(), _queryContext).ConfigureAwait(false))
          {
            rootInCache = AddToCache(BuildPublishable(item2), rootInCache);
          }
        }
        return YieldAncestorChain(rootInCache);
      }
      finally
      {
        _ancestorLock.Release();
      }
    }

    public async Task<IEnumerable<IPublishCandidate>> GetChildren(IReadOnlyCollection<Guid> parentIds, int skip, int take)
    {
      return (await _itemReadRepo.GetChildNodes(parentIds, _queryContext, skip, take).ConfigureAwait(false)).Select(BuildPublishable).ToArray();
    }

    public async Task<IEnumerable<Guid>> GetChildIds(IReadOnlyCollection<Guid> parentIds)
    {
      return await _itemReadRepo.GetChildIds(parentIds);
    }

    public async Task<IEnumerable<IPublishCandidate>> GetRelatedNodes(IReadOnlyCollection<IItemVariantIdentifier> locators, bool includeRelatedContent, bool includeClones)
    {
      HashSet<ItemRelationshipType> inRelsFilter = new HashSet<ItemRelationshipType>();
      HashSet<ItemRelationshipType> outRelsFilter = new HashSet<ItemRelationshipType>();
      if (includeRelatedContent)
      {
        outRelsFilter.Add(ItemRelationshipType.CloneOf);
        outRelsFilter.Add(ItemRelationshipType.CloneVersionOf);
        outRelsFilter.Add(ItemRelationshipType.DefaultedBy);
        outRelsFilter.Add(ItemRelationshipType.InheritsFrom);
        outRelsFilter.Add(ItemRelationshipType.TemplatedBy);
        outRelsFilter.Add(ItemRelationshipType.ContentComposedOf);
        if (includeClones)
        {
          inRelsFilter.Add(ItemRelationshipType.CloneOf);
          inRelsFilter.Add(ItemRelationshipType.CloneVersionOf);
        }
      }
      if (!inRelsFilter.Any() && !outRelsFilter.Any())
      {
        return Enumerable.Empty<IPublishCandidate>();
      }
      List<IItemVariantIdentifier> itemLocators = locators.ToList();
      Guid[] ids = (!outRelsFilter.Any() || inRelsFilter.Any()) ? ((!inRelsFilter.Any() || outRelsFilter.Any()) ? (await _itemRelationshipRepo.GetAllRelationships(_sourceStore, itemLocators, outRelsFilter, inRelsFilter)).SelectMany((KeyValuePair<IItemVariantIdentifier, ItemVertexMetadata> x) => (from r in x.Value.Out
                                                                                                                                                                                                                                                                                                         select r.TargetId).Concat(from r in x.Value.In
                                                                                                                                                                                                                                                                                                                                   select r.SourceId)).Distinct().ToArray() : (await _itemRelationshipRepo.GetInRelationships(_sourceStore, itemLocators, inRelsFilter).ConfigureAwait(false)).SelectMany((KeyValuePair<IItemVariantIdentifier, IReadOnlyCollection<IItemRelationship>> x) => from r in x.Value
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              select r.SourceId).Distinct().ToArray()) : (await _itemRelationshipRepo.GetOutRelationships(_sourceStore, itemLocators, outRelsFilter).ConfigureAwait(false)).SelectMany((KeyValuePair<IItemVariantIdentifier, IReadOnlyCollection<IItemRelationship>> x) => from r in x.Value
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           select r.SourceId).Distinct().ToArray();
      return (await _itemReadRepo.GetItemNodes((IReadOnlyCollection<Guid>)ids, _queryContext).ConfigureAwait(false)).Select(BuildPublishable).ToArray();
    }

    private CacheablePublishable GetFromCache(Guid id)
    {
      if (_ancestorCache.TryGetValue(id, out CacheablePublishable value))
      {
        return value;
      }
      return null;
    }

    private CacheablePublishable AddToCache(IPublishCandidate node, CacheablePublishable parent)
    {
      CacheablePublishable cacheablePublishable = new CacheablePublishable(node, parent);
      _ancestorCache.Add(node.Id, cacheablePublishable);
      return cacheablePublishable;
    }

    private IEnumerable<IPublishCandidate> YieldAncestorChain(CacheablePublishable node)
    {
      List<IPublishCandidate> list = new List<IPublishCandidate>();
      while (node != null)
      {
        list.Add(node.Node);
        node = node.Parent;
      }
      list.Reverse();
      return list;
    }

    private IPublishCandidate BuildPublishable(IItemNode node)
    {
      IReadOnlyCollection<IFieldData> readOnlyCollection = _publishingTemplateGraph.GetStandardValues(node.Properties.TemplateId).EnsureEnumerated();
      IReadOnlyCollection<IFieldData> fieldsToMerge = (IReadOnlyCollection<IFieldData>)new IFieldData[0];
      bool isMedia = IsMediaItem(node);
      bool flag = IsClonedItem(node);
      if (flag)
      {
        ParseSitecoreItemUriField(node.InvariantFields, PublishingConstants.Clones.SourceItem);
        IFieldData[] sourceNodeFieldsData = GetCloneSourceNodeFieldsData(node).Result.ToArray();
        if (sourceNodeFieldsData.Any())
        {
          IEnumerable<IFieldData> second = from x in readOnlyCollection
                                           where sourceNodeFieldsData.All((IFieldData s) => s.FieldId != x.FieldId)
                                           select x;
          fieldsToMerge = (IReadOnlyCollection<IFieldData>)sourceNodeFieldsData.Concat(second).ToArray();
        }
      }
      else
      {
        fieldsToMerge = readOnlyCollection;
      }
      IItemNode node2 = MergeFieldDataValues(node, fieldsToMerge);
      ItemPublishRestrictions itemPublishRestrictions = ExtractSharedRestrictions(node2, _contentAvailabilityEnabled);
      return new PublishCandidate(node, itemPublishRestrictions, ExtractVariantRestrictions(node2, itemPublishRestrictions), isMedia, flag);
    }

    private bool IsClonedItem(IItemNode node)
    {
      return node.InvariantFields.Any(delegate (IFieldData x)
      {
        if (x.FieldId == PublishingConstants.Clones.SourceItem)
        {
          return !string.IsNullOrWhiteSpace(x.RawValue);
        }
        return false;
      });
    }

    private bool IsMediaItem(IItemNode node)
    {
      Guid mediaId;
      return (from x in node.InvariantFields
              where _mediaFieldsIds.Contains(x.FieldId)
              select x).Concat(from x in node.LanguageVariantFields.SelectMany((KeyValuePair<Language, IReadOnlyCollection<IFieldData>> x) => x.Value)
                               where _mediaFieldsIds.Contains(x.FieldId)
                               select x).Concat(from x in node.VariantFields.SelectMany((KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> x) => x.Value)
                                                where _mediaFieldsIds.Contains(x.FieldId)
                                                select x).Any((IFieldData f) => Guid.TryParse(f.RawValue, out mediaId));
    }

    private async Task<IEnumerable<IFieldData>> GetCloneSourceNodeFieldsData(IItemNode node)
    {
      List<IFieldData> sourcesFieldData = new List<IFieldData>();
      IItemNode itemNode;
      do
      {
        IItemLocator itemLocator = ParseSitecoreItemUriField(node.InvariantFields, PublishingConstants.Clones.SourceItem);
        itemNode = await _itemReadRepo.GetItemNode(itemLocator.Id, _queryContext).ConfigureAwait(false);
        if (itemNode == null)
        {
          break;
        }
        IEnumerable<IFieldData> source = itemNode.VariantFields.SelectMany((KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> x) => x.Value).Concat(itemNode.InvariantFields).Concat(itemNode.LanguageVariantFields.SelectMany((KeyValuePair<Language, IReadOnlyCollection<IFieldData>> x) => x.Value));
        List<IFieldData> list = sourcesFieldData;
        source.ForEach(list.Add);
        node = itemNode;
      }
      while (IsClonedItem(itemNode));
      return sourcesFieldData.AsEnumerable();
    }

   private static IItemNode MergeFieldDataValues(IItemNode node, IReadOnlyCollection<IFieldData> fieldsToMerge)
        {
            bool fieldNotPresent(IEnumerable<IFieldData> fields, IFieldData searchField) =>
               !fields.Any(nodeField => nodeField.FieldId == searchField.FieldId);

            bool varianceMatches(IEnumerable<IFieldData> fields, IFieldData searchField) =>
                fields.Any(nodeField => nodeField.Variance.Version == searchField.Variance.Version &&
                                         nodeField.Variance.Language == searchField.Variance.Language);

            var fieldsValidToMerge = fieldsToMerge
                .GroupBy(f => f.Variance.VarianceType)
                .SelectMany(g =>
                {
                    switch (g.Key)
                    {
                        case VarianceType.Invariant:
                            return g.Where(f => fieldNotPresent(node.InvariantFields, f));

                        case VarianceType.LanguageVariant:
                            return node.LanguageVariantFields
                                .SelectMany(kv =>
                                {
                                    var langVariance = VarianceInfo.LanguageVariant(kv.Key);
                                    return g
                                        .Where(f => varianceMatches(kv.Value, f) && fieldNotPresent(kv.Value, f))
                                        .Select(f => new FieldData(f.FieldId, node.Id, f.RawValue, langVariance));
                                });
                        default:
                            return node.VariantFields
                                .SelectMany(kv =>
                                {
                                    var info = kv.Key.AsInfo();
                                    return g
                                        .Where(f => varianceMatches(kv.Value, f) && fieldNotPresent(kv.Value, f))
                                        .Select(f => new FieldData(f.FieldId, node.Id, f.RawValue, info));
                                });
                    }
                })
                .ToArray();

            return node.Clone(fieldsValidToMerge);
        }

    private static ItemPublishRestrictions ExtractSharedRestrictions(IItemNode node, bool contentAvailabilityEnabled)
    {
      IEnumerable<Guid> validTargets = ParseSitecoreMultipleGuidField(node.InvariantFields, PublishingConstants.PublishingFields.Shared.PublishingTargets);
      bool value = !ParseSitecoreBoolField(node.InvariantFields, PublishingConstants.PublishingFields.Shared.NeverPublish, false);
      DateTime value2 = ParseSitecoreDateField(node.InvariantFields, PublishingConstants.PublishingFields.Shared.PublishDate, MinUtc);
      DateTime value3 = ParseSitecoreDateField(node.InvariantFields, PublishingConstants.PublishingFields.Shared.UnpublishDate, MaxUtc);
      return new ItemPublishRestrictions(workflow: ParseSitecoreGuidField(node.InvariantFields, PublishingConstants.WorkflowFields.Workflow, null), validTargets: validTargets, isPublishable: value, contentAvailabilityEnabled: contentAvailabilityEnabled, sunrise: value2, sunset: value3);
    }

    private Dictionary<IVarianceIdentifier, VariantPublishRestrictions> ExtractVariantRestrictions(IItemNode node, ItemPublishRestrictions itemPublishRestrictions)
    {
      Dictionary<IVarianceIdentifier, VariantPublishRestrictions> dictionary = new Dictionary<IVarianceIdentifier, VariantPublishRestrictions>(IdentifierComparer);
      KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>>[] variantFields = node.VariantFields.ToArray();
      int i;
      KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> variant;
      bool isPublishable;
      Predicate<Guid> inPublishableStateForTarget;
      DateTime dateTime;
      DateTime dateTime2;
      for (i = 0; i < node.VariantFields.Count; dictionary.Add(variant.Key, new VariantPublishRestrictions(isPublishable, _contentAvailabilityEnabled, inPublishableStateForTarget, dateTime, dateTime2)), i++)
      {
        variant = variantFields[i];
        isPublishable = !ParseSitecoreBoolField(variant.Value, PublishingConstants.PublishingFields.Versioned.HideVersion, false);
        inPublishableStateForTarget = IsInPublishableWorkflowStateForTarget(itemPublishRestrictions, ref variant);
        dateTime = ParseSitecoreDateField(variant.Value, PublishingConstants.PublishingFields.Versioned.ValidFrom, MinUtc);
        dateTime2 = ParseSitecoreDateField(variant.Value, PublishingConstants.PublishingFields.Versioned.ValidTo, MaxUtc);
        List<KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>>> list = variantFields.Where(delegate (KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> x)
        {
          if (x.Key.Language.Equals(variantFields[i].Key.Language))
          {
            return x.Key.Version > variantFields[i].Key.Version;
          }
          return false;
        }).ToList();
        if (_contentAvailabilityEnabled && DateTime.Equals(dateTime2, MaxUtc) && list.Any())
        {
          KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> keyValuePair = list.FirstOrDefault((KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> x) => x.Key.Version == (Sitecore.Framework.Publishing.Item.Version)((int)variantFields[i].Key.Version + 1));
          if (keyValuePair.Value != null)
          {
            Guid? nullable = ParseSitecoreGuidField(keyValuePair.Value, PublishingConstants.WorkflowFields.WorkflowState, null);
            if (!nullable.HasValue || _publishableStates.ContainsKey(nullable.Value))
            {
              dateTime2 = ParseSitecoreDateField(keyValuePair.Value, PublishingConstants.PublishingFields.Versioned.ValidFrom, MaxUtc);
            }
            if (VarianceOverriddenByNewerVariance(list, dateTime) && dateTime2 != MaxUtc)
            {
              isPublishable = false;
            }
          }
        }
        if (_contentAvailabilityEnabled)
        {
          if (dateTime < itemPublishRestrictions.Sunrise && dateTime2 < itemPublishRestrictions.Sunrise)
          {
            goto IL_01c0;
          }
          if (dateTime > itemPublishRestrictions.Sunset && dateTime2 > itemPublishRestrictions.Sunset)
          {
            goto IL_01c0;
          }
        }
        continue;
      IL_01c0:
        isPublishable = false;
      }
      return dictionary;
    }

    private Predicate<Guid> IsInPublishableWorkflowStateForTarget(ItemPublishRestrictions itemPublishRestrictions, ref KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> variant)
    {
      Guid? nullable = ParseSitecoreGuidField(variant.Value, PublishingConstants.WorkflowFields.WorkflowState, null);
      Predicate<Guid> result = (Guid target) => true;
      if (itemPublishRestrictions.Workflow.HasValue && nullable.HasValue)
      {
        result = ((!_publishableStates.TryGetValue(nullable.Value, out IPublishableWorkflowState targetState)) ? ((Predicate<Guid>)((Guid target) => false)) : ((Predicate<Guid>)((Guid target) => targetState.IsPublishableFor(target))));
      }
      return result;
    }

    private bool VarianceOverriddenByNewerVariance(IEnumerable<KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>>> nextVariances, DateTime varianceValidFrom)
    {
      return nextVariances.Any((KeyValuePair<IVarianceIdentifier, IReadOnlyCollection<IFieldData>> nextVariant) => ParseSitecoreDateField(nextVariant.Value, PublishingConstants.PublishingFields.Versioned.ValidFrom, MaxUtc) < varianceValidFrom);
    }

    private static bool ParseSitecoreBoolField(IEnumerable<IFieldData> fields, Guid fieldId, bool defaultValue)
    {
      bool result = defaultValue;
      IFieldData fieldData = fields.FirstOrDefault((IFieldData f) => f.FieldId == fieldId);
      if (fieldData != null && fieldData.RawValue != null)
      {
        result = (fieldData.RawValue == "1");
      }
      return result;
    }

    private static DateTime ParseSitecoreDateField(IEnumerable<IFieldData> fields, Guid fieldId, DateTime defaultValue)
    {
      DateTime result = defaultValue;
      IFieldData fieldData = fields.FirstOrDefault((IFieldData f) => f.FieldId == fieldId);
      if (fieldData != null)
      {
        result = ClassicDateUtil.ParseDateTime(fieldData.RawValue, defaultValue);
      }
      return result;
    }

    private static Guid? ParseSitecoreGuidField(IEnumerable<IFieldData> fields, Guid fieldId, Guid? defaultValue)
    {
      IFieldData fieldData = fields.FirstOrDefault((IFieldData f) => f.FieldId == fieldId);
      if (fieldData != null && fieldData.RawValue != null && Guid.TryParse(fieldData.RawValue, out Guid result))
      {
        return result;
      }
      return defaultValue;
    }

    private static IItemLocator ParseSitecoreItemUriField(IEnumerable<IFieldData> fields, Guid fieldId)
    {
      IFieldData fieldData = fields.FirstOrDefault((IFieldData f) => f.FieldId == fieldId);
      if (fieldData != null && fieldData.RawValue != null)
      {
        return ItemLocatorUtils.ParseSitecoreItemUri(fieldData.RawValue, "not_important");
      }
      return null;
    }

    private static IEnumerable<Guid> ParseSitecoreMultipleGuidField(IEnumerable<IFieldData> fields, Guid fieldId)
    {
      IFieldData fieldData = fields.FirstOrDefault((IFieldData f) => f.FieldId == fieldId);
      if (fieldData != null && fieldData.RawValue != null)
      {
        string[] array = fieldData.RawValue.Split(new char[1]
        {
                '|'
        }, StringSplitOptions.RemoveEmptyEntries);
        for (int i = 0; i < array.Length; i++)
        {
          if (Guid.TryParse(array[i], out Guid result))
          {
            yield return result;
          }
        }
      }
    }

    static PublishCandidateSource()
    {
      DateTime dateTime = DateTime.MaxValue;
      MaxUtc = dateTime.ToUniversalTime();
      dateTime = DateTime.MinValue;
      MinUtc = dateTime.ToUniversalTime();
      IdentifierComparer = new IVarianceIdentifierComparer();
    }
  }
}